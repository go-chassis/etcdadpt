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
	"fmt"
	"time"

	"github.com/go-chassis/foundation/backoff"

	"github.com/little-cui/etcdadpt/middleware/log"
	"github.com/little-cui/etcdadpt/middleware/metrics"
)

func (c *Client) HealthCheck() {
	if c.AutoSyncInterval >= time.Second {
		c.goroutine.Do(c.HealthCheckLoop)
	}
}

func (c *Client) HealthCheckLoop(ctx context.Context) {
	var lastErr error

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.AutoSyncInterval):
			err := c.autoSync(ctx)
			if err == nil && lastErr != nil {
				c.onConnected()
			} else if err != nil && lastErr == nil {
				c.onError(err)
			}
			lastErr = err
		}

		if lastErr == nil {
			continue
		}

		log.GetLogger().Error(fmt.Sprintf("etcd health check failed, error: %s", lastErr))
		if err := c.ReOpen(); err != nil {
			log.GetLogger().Error(fmt.Sprintf("re-connect to etcd failed, error: %s", err))
		}
	}
}

func (c *Client) autoSync(ctx context.Context) (err error) {
	for i := 0; i < healthCheckRetryTimes; i++ {
		subCtx, cancel := context.WithTimeout(c.Client.Ctx(), healthCheckTimeout)
		err = c.SyncMembers(subCtx)
		cancel()
		if err == nil {
			return
		}
		d := backoff.GetBackoff().Delay(i)
		log.GetLogger().Error(fmt.Sprintf("retry to sync members from etcd %s after %s, error: %s", c.Endpoints, d, err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d):
		}
	}
	return
}

func (c *Client) SyncMembers(ctx context.Context) error {
	var err error

	start := time.Now()
	defer metrics.ReportBackendOperationCompleted(OperationSyncMembers, err, start)

	if err = c.Client.Sync(ctx); err != nil && err != c.Client.Ctx().Err() {
		return err
	}
	return nil
}
