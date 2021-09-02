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

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/metrics"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *Client) Txn(ctx context.Context, opts []etcdadpt.OpOptions) (*etcdadpt.Response, error) {
	resp, err := c.TxnWithCmp(ctx, opts, nil, nil)
	if err != nil {
		return nil, err
	}
	return &etcdadpt.Response{
		Succeeded: resp.Succeeded,
		Revision:  resp.Revision,
	}, nil
}

func (c *Client) TxnWithCmp(ctx context.Context, success []etcdadpt.OpOptions, cmps []etcdadpt.CmpOptions, fail []etcdadpt.OpOptions) (*etcdadpt.Response, error) {
	var err error

	start := time.Now()
	etcdCmps := c.toCompares(cmps)
	etcdSuccessOps := c.toTxnRequest(success)
	etcdFailOps := c.toTxnRequest(fail)

	var traceOps []etcdadpt.OpOptions
	traceOps = append(traceOps, success...)
	traceOps = append(traceOps, fail...)
	if len(traceOps) == 0 {
		return nil, fmt.Errorf("requested success or fail OpOptions list")
	}

	span := TracingBegin(ctx, "etcd:txn", traceOps[0])
	otCtx, cancel := c.WithTimeout(ctx)
	defer func() {
		metrics.ReportBackendOperationCompleted(OperationTxn, err, start)
		TracingEnd(span, err)
		cancel()
	}()

	kvc := clientv3.NewKV(c.Client)
	txn := kvc.Txn(otCtx)
	if len(etcdCmps) > 0 {
		txn.If(etcdCmps...)
	}
	txn.Then(etcdSuccessOps...)
	if len(etcdFailOps) > 0 {
		txn.Else(etcdFailOps...)
	}
	resp, err := txn.Commit()
	if err != nil {
		if err.Error() == rpctypes.ErrKeyNotFound.Error() {
			// etcd return ErrKeyNotFound if key does not exist and
			// the PUT options contain WithIgnoreLease
			return &etcdadpt.Response{Succeeded: false}, nil
		}
		return nil, err
	}
	c.logNilOrWarn(start, fmt.Sprintf("registry client txn {if(%v): %s, then: %d, else: %d}, rev: %d",
		resp.Succeeded, cmps, len(success), len(fail), resp.Header.Revision))

	var rangeResponse etcdserverpb.RangeResponse
	for _, itf := range resp.Responses {
		if rr, ok := itf.Response.(*etcdserverpb.ResponseOp_ResponseRange); ok {
			// plz request the same type range kv in txn success/fail options
			rangeResponse.Kvs = append(rangeResponse.Kvs, rr.ResponseRange.Kvs...)
			rangeResponse.Count += rr.ResponseRange.Count
		}
	}

	return &etcdadpt.Response{
		Succeeded: resp.Succeeded,
		Revision:  resp.Header.Revision,
		Kvs:       rangeResponse.Kvs,
		Count:     rangeResponse.Count,
	}, nil
}
