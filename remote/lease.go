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
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/go-chassis/foundation/stringutil"
	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/metrics"
)

func (c *Client) LeaseGrant(ctx context.Context, TTL int64) (int64, error) {
	var err error

	start := time.Now()
	span := TracingBegin(ctx, "etcd:grant",
		etcdadpt.OpOptions{Action: etcdadpt.ActionPut, Key: stringutil.Str2bytes(strconv.FormatInt(TTL, 10))})
	otCtx, cancel := c.WithTimeout(ctx)
	defer func() {
		metrics.ReportBackendOperationCompleted(OperationLeaseGrant, err, start)
		TracingEnd(span, err)
		cancel()
	}()
	etcdResp, err := c.Client.Grant(otCtx, TTL)
	if err != nil {
		return 0, err
	}
	c.logNilOrWarn(start, fmt.Sprintf("registry client grant lease %ds", TTL))
	return int64(etcdResp.ID), nil
}

func (c *Client) LeaseRenew(ctx context.Context, leaseID int64) (int64, error) {
	var err error

	start := time.Now()
	span := TracingBegin(ctx, "etcd:keepalive",
		etcdadpt.OpOptions{Action: etcdadpt.ActionPut, Key: stringutil.Str2bytes(strconv.FormatInt(leaseID, 10))})
	otCtx, cancel := c.WithTimeout(ctx)
	defer func() {
		metrics.ReportBackendOperationCompleted(OperationLeaseRenew, err, start)
		TracingEnd(span, err)
		cancel()
	}()

	etcdResp, err := c.Client.KeepAliveOnce(otCtx, clientv3.LeaseID(leaseID))
	if err != nil {
		if err.Error() == rpctypes.ErrLeaseNotFound.Error() {
			return 0, etcdadpt.ErrLeaseNotFound
		}
		return 0, err
	}
	c.logNilOrWarn(start, fmt.Sprintf("registry client renew lease %d", leaseID))
	return etcdResp.TTL, nil
}

func (c *Client) LeaseRevoke(ctx context.Context, leaseID int64) error {
	var err error

	start := time.Now()
	span := TracingBegin(ctx, "etcd:revoke",
		etcdadpt.OpOptions{Action: etcdadpt.ActionDelete, Key: stringutil.Str2bytes(strconv.FormatInt(leaseID, 10))})
	otCtx, cancel := c.WithTimeout(ctx)
	defer func() {
		metrics.ReportBackendOperationCompleted(OperationLeaseRevoke, err, start)
		TracingEnd(span, err)
		cancel()
	}()

	_, err = c.Client.Revoke(otCtx, clientv3.LeaseID(leaseID))
	if err != nil {
		if err.Error() == rpctypes.ErrLeaseNotFound.Error() {
			return etcdadpt.ErrLeaseNotFound
		}
		return err
	}
	c.logNilOrWarn(start, fmt.Sprintf("registry client revoke lease %d", leaseID))
	return nil
}
