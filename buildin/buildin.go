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

package buildin

import (
	"context"

	"github.com/little-cui/etcdadpt"
)

var (
	closeCh    = make(chan struct{})
	noResponse = &etcdadpt.Response{}
)

func init() {
	close(closeCh)
	etcdadpt.Install("buildin", NewClient)
}

type Client struct {
	ready chan int
}

func (ec *Client) Err() (err <-chan error) {
	return
}

func (ec *Client) Ready() <-chan struct{} {
	return closeCh
}
func (ec *Client) Do(ctx context.Context, opts ...etcdadpt.OpOption) (*etcdadpt.Response, error) {
	return noResponse, nil
}
func (ec *Client) Txn(ctx context.Context, ops []etcdadpt.OpOptions) (*etcdadpt.Response, error) {
	return noResponse, nil
}
func (ec *Client) TxnWithCmp(ctx context.Context, success []etcdadpt.OpOptions, cmp []etcdadpt.CmpOptions, fail []etcdadpt.OpOptions) (*etcdadpt.Response, error) {
	return noResponse, nil
}
func (ec *Client) LeaseGrant(ctx context.Context, TTL int64) (leaseID int64, err error) {
	return 0, nil
}
func (ec *Client) LeaseRenew(ctx context.Context, leaseID int64) (TTL int64, err error) {
	return 0, nil
}
func (ec *Client) LeaseRevoke(ctx context.Context, leaseID int64) error {
	return nil
}
func (ec *Client) Watch(ctx context.Context, opts ...etcdadpt.OpOption) error {
	return nil
}
func (ec *Client) Compact(ctx context.Context, reserve int64) error {
	return nil
}
func (ec *Client) ListCluster(ctx context.Context) (etcdadpt.Clusters, error) {
	return nil, nil
}
func (ec *Client) Status(ctx context.Context) (*etcdadpt.StatusResponse, error) {
	return &etcdadpt.StatusResponse{}, nil
}
func (ec *Client) Close() {
}

func NewClient(cfg etcdadpt.Config) etcdadpt.Client {
	return &Client{
		ready: make(chan int),
	}
}
