/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package remote

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-chassis/foundation/gopool"
	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/metrics"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var FirstEndpoint string

func init() {
	etcdadpt.Install("etcd", NewClient)
}

type Client struct {
	Cfg    etcdadpt.Config
	Client *clientv3.Client

	Endpoints        []string
	DialTimeout      time.Duration
	AutoSyncInterval time.Duration

	err       chan error
	ready     chan struct{}
	goroutine *gopool.Pool
}

func (c *Client) WithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.Cfg.RequestTimeOut)
}

func (c *Client) Initialize() (err error) {
	c.err = make(chan error, 1)
	c.ready = make(chan struct{})
	c.goroutine = gopool.New(gopool.Configure().WithRecoverFunc(c.logRecover))

	if len(c.Endpoints) == 0 {
		// parse the endpoints from config
		c.parseEndpoints()
	}

	if c.Cfg.TLSConfig == nil && c.Cfg.SslEnabled {
		return errors.New("required TLSConfig")
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = c.Cfg.DialTimeout
	}
	if c.AutoSyncInterval == 0 {
		c.AutoSyncInterval = c.Cfg.AutoSyncInterval
	}

	c.Client, err = c.newClient()
	if err != nil {
		c.logger().Error(fmt.Sprintf("get etcd client %v failed. error: %s", c.Endpoints, err))
		c.onError(err)
		return
	}
	c.onConnected()

	c.HealthCheck()

	close(c.ready)

	c.logger().Warn(fmt.Sprintf("get etcd client %v completed, ssl: %v, dial timeout: %s, auto sync endpoints interval is %s.",
		c.Endpoints, c.Cfg.TLSConfig != nil, c.DialTimeout, c.AutoSyncInterval))
	return
}

func (c *Client) newClient() (*clientv3.Client, error) {
	inst, err := clientv3.New(clientv3.Config{
		Endpoints:            c.Endpoints,
		DialTimeout:          c.DialTimeout,
		TLS:                  c.Cfg.TLSConfig,
		MaxCallSendMsgSize:   maxSendMsgSize,
		MaxCallRecvMsgSize:   maxRecvMsgSize,
		DialKeepAliveTime:    keepAliveTime,
		DialKeepAliveTimeout: keepAliveTimeout,
		AutoSyncInterval:     0, // DON'T start auto sync, duplicate with c.HealthCheck()
	})
	defer func() {
		if err == nil {
			return
		}
		if inst != nil {
			inst.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(inst.Ctx(), healthCheckTimeout)
	defer cancel()
	resp, err := inst.MemberList(ctx)
	if err != nil {
		return nil, err
	}

	metrics.ReportBackendInstance(len(resp.Members))

	if len(c.Endpoints) == 1 {
		// no need to check remote endpoints
		return inst, nil
	}

epLoop:
	for _, ep := range c.Endpoints {
		var cluster []string
		for _, mem := range resp.Members {
			for _, curl := range mem.ClientURLs {
				u, err := url.Parse(curl)
				if err != nil {
					return nil, err
				}
				cluster = append(cluster, u.Host)
				if u.Host == ep {
					continue epLoop
				}
			}
		}
		// maybe endpoints = [domain A, domain B] or there are more than one cluster
		err = fmt.Errorf("the etcd cluster endpoint list%v does not contain %s", cluster, ep)
		return nil, err
	}

	return inst, nil
}

func (c *Client) ReOpen() error {
	client, cerr := c.newClient()
	if cerr != nil {
		c.logger().Error(fmt.Sprintf("create a new connection to etcd %v failed, error: %s",
			c.Endpoints, cerr))
		return cerr
	}
	c.Client, client = client, c.Client
	if cerr = client.Close(); cerr != nil {
		c.logger().Error(fmt.Sprintf("failed to close the unavailable etcd client, error: %s", cerr))
	}
	client = nil
	return nil
}

func (c *Client) parseEndpoints() {
	// use the default cluster endpoints
	addrs := etcdadpt.GetClusterURL(c.Cfg.ClusterName, c.Cfg.ClusterAddresses, "")

	endpoints := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Index(addr, "://") > 0 {
			// 如果配置格式为"sr-0=http(s)://IP:Port"，则需要分离IP:Port部分
			endpoints = append(endpoints, addr[strings.Index(addr, "://")+3:])
		} else {
			endpoints = append(endpoints, addr)
		}
	}
	c.Endpoints = endpoints

	c.logger().Info(fmt.Sprintf("parse %s -> endpoints: %v, ssl: %v",
		c.Cfg.ClusterAddresses, c.Endpoints, c.Cfg.SslEnabled))
}

func (c *Client) onError(err error) {
	if c.Cfg.ErrorFunc == nil {
		return
	}
	c.Cfg.ErrorFunc(err)
}

func (c *Client) onConnected() {
	if c.Cfg.ConnectedFunc == nil {
		return
	}
	c.Cfg.ConnectedFunc()
}

func NewClient(cfg etcdadpt.Config) etcdadpt.Client {
	inst := &Client{
		Cfg: cfg,
	}
	logger := inst.logger()
	logger.Warn("enable remote registry mode")

	if err := inst.Initialize(); err != nil {
		inst.err <- err
		return inst
	}

	scheme := "http://"
	if cfg.TLSConfig != nil {
		scheme = "https://"
	}
	FirstEndpoint = scheme + inst.Endpoints[0]

	return inst
}
