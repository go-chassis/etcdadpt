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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/log"
	"github.com/little-cui/etcdadpt/middleware/metrics"
)

var ErrGetLeaderFailed = errors.New("get leader failed")

func (c *Client) Compact(ctx context.Context, reserve int64) error {
	eps := c.Client.Endpoints()
	curRev := c.getLeaderCurrentRevision(ctx)

	revToCompact := max(0, curRev-reserve)
	if revToCompact <= 0 {
		log.GetLogger().Info(fmt.Sprintf("revision is %d, <=%d, no nead to compact %s", curRev, reserve, eps))
		return nil
	}

	t := time.Now()
	_, err := c.Client.Compact(ctx, revToCompact, clientv3.WithCompactPhysical())
	metrics.ReportBackendOperationCompleted(OperationCompact, err, t)
	if err != nil {
		log.GetLogger().Error(fmt.Sprintf("compact %s failed, revision is %d(current: %d, reserve %d), error: %s",
			eps, revToCompact, curRev, reserve, err))
		return err
	}
	c.logInfoOrWarn(t, fmt.Sprintf("compacted %s, revision is %d(current: %d, reserve %d)",
		eps, revToCompact, curRev, reserve))
	return nil
}

func (c *Client) getLeaderCurrentRevision(ctx context.Context) int64 {
	curRev := int64(0)
	ep, resp := c.getLeaderStatus(ctx)
	if len(ep) == 0 || resp == nil {
		return 0
	}
	curRev = resp.Header.Revision
	log.GetLogger().Info(fmt.Sprintf("get leader endpoint: %s, revision is %d", ep, curRev))
	return curRev
}

func (c *Client) getLeaderStatus(ctx context.Context) (string, *clientv3.StatusResponse) {
	eps := c.Client.Endpoints()
	for _, ep := range eps {
		resp, err := c.GetEndpointStatus(ctx, ep)
		if err != nil {
			log.GetLogger().Error(fmt.Sprintf("compact error ,can not get status from %s, error: %s", ep, err))
			continue
		}
		if resp.Leader == resp.Header.MemberId {
			return ep, resp
		}
	}
	return "", nil
}

func (c *Client) GetEndpointStatus(ctx context.Context, ep string) (*clientv3.StatusResponse, error) {
	otCtx, cancel := c.WithTimeout(ctx)
	resp, err := c.Client.Status(otCtx, ep)
	defer cancel()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ListCluster(ctx context.Context) (etcdadpt.Clusters, error) {
	clusters := etcdadpt.ParseClusters(c.Cfg.ClusterName, c.Cfg.ClusterAddresses, c.Cfg.ManagerAddress)
	return clusters, nil
}

func (c *Client) Status(ctx context.Context) (*etcdadpt.StatusResponse, error) {
	ep, resp := c.getLeaderStatus(ctx)
	if len(ep) == 0 || resp == nil {
		return nil, ErrGetLeaderFailed
	}
	return &etcdadpt.StatusResponse{
		DBSize: resp.DbSize,
	}, nil
}

func (c *Client) Err() <-chan error {
	return c.err
}

func (c *Client) Ready() <-chan struct{} {
	return c.ready
}

func (c *Client) Close() {
	c.goroutine.Close(true)

	if c.Client != nil {
		c.Client.Close()
	}
	log.GetLogger().Debug("etcd client stopped")
}
