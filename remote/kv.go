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

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/go-chassis/foundation/stringutil"
	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/metrics"
)

func (c *Client) Do(ctx context.Context, opts ...etcdadpt.OpOption) (*etcdadpt.Response, error) {
	var (
		err  error
		resp *etcdadpt.Response
	)

	start := time.Now()
	op := etcdadpt.OptionsToOp(opts...)
	span := TracingBegin(ctx, "etcd:do", op)
	otCtx, cancel := c.WithTimeout(ctx)
	defer func() {
		metrics.ReportBackendOperationCompleted(op.Action.String(), err, start)
		TracingEnd(span, err)
		cancel()
	}()

	switch op.Action {
	case etcdadpt.ActionGet:
		var etcdResp *clientv3.GetResponse
		key := stringutil.Bytes2str(op.Key)

		if (op.Prefix || len(op.EndKey) > 0) && !op.CountOnly {
			etcdResp, err = c.Paging(ctx, op)
			if err != nil {
				break
			}
		}

		if etcdResp == nil {
			etcdResp, err = c.Client.Get(otCtx, key, c.toGetRequest(op)...)
			if err != nil {
				break
			}
		}

		resp = &etcdadpt.Response{
			Kvs:      etcdResp.Kvs,
			Count:    etcdResp.Count,
			Revision: etcdResp.Header.Revision,
		}
	case etcdadpt.ActionPut:
		var value string
		if len(op.Value) > 0 {
			value = stringutil.Bytes2str(op.Value)
		}
		var etcdResp *clientv3.PutResponse
		etcdResp, err = c.Client.Put(otCtx, stringutil.Bytes2str(op.Key), value, c.toPutRequest(op)...)
		if err != nil {
			break
		}
		resp = &etcdadpt.Response{
			Revision: etcdResp.Header.Revision,
		}
	case etcdadpt.ActionDelete:
		var etcdResp *clientv3.DeleteResponse
		etcdResp, err = c.Client.Delete(otCtx, stringutil.Bytes2str(op.Key), c.toDeleteRequest(op)...)
		if err != nil {
			break
		}
		resp = &etcdadpt.Response{
			Revision: etcdResp.Header.Revision,
		}
	}

	if err != nil {
		return nil, err
	}

	resp.Succeeded = true

	c.logNilOrWarn(start, fmt.Sprintf("registry client do %s", op))
	return resp, nil
}

func (c *Client) Paging(ctx context.Context, op etcdadpt.OpOptions) (*clientv3.GetResponse, error) {
	var etcdResp *clientv3.GetResponse
	key := stringutil.Bytes2str(op.Key)

	start := time.Now()
	tempOp := op
	tempOp.CountOnly = true
	countResp, err := c.Client.Get(ctx, key, c.toGetRequest(tempOp)...)
	if err != nil {
		return nil, err
	}

	recordCount := countResp.Count
	if op.Offset == -1 && recordCount <= op.Limit {
		return nil, nil // no need to do paging
	}

	tempOp.CountOnly = false
	tempOp.Prefix = false
	tempOp.SortOrder = etcdadpt.SortAscend
	tempOp.EndKey = op.EndKey
	if len(op.EndKey) == 0 {
		tempOp.EndKey = stringutil.Str2bytes(clientv3.GetPrefixRangeEnd(key))
	}
	tempOp.Revision = countResp.Header.Revision

	etcdResp = countResp
	etcdResp.Kvs = make([]*mvccpb.KeyValue, 0, etcdResp.Count)

	pageCount := recordCount / op.Limit
	remainCount := recordCount % op.Limit
	if remainCount > 0 {
		pageCount++
	}
	minPage, maxPage := int64(0), pageCount
	if op.Offset >= 0 {
		count := op.Offset + 1
		maxPage = count / op.Limit
		if count%op.Limit > 0 {
			maxPage++
		}
		minPage = maxPage - 1
	}

	var baseOps []clientv3.OpOption
	baseOps = append(baseOps, c.toGetRequest(tempOp)...)

	nextKey := key
	for i := int64(0); i < pageCount; i++ {
		if i >= maxPage {
			break
		}

		limit, start := op.Limit, 0
		if remainCount > 0 && i == pageCount-1 {
			limit = remainCount
		}
		if i != 0 {
			limit++
			start = 1
		}
		ops := append(baseOps, clientv3.WithLimit(limit))
		recordResp, err := c.Client.Get(ctx, nextKey, ops...)
		if err != nil {
			return nil, err
		}

		l := int64(len(recordResp.Kvs))
		if l <= 0 { // no more data, data may decrease during paging
			break
		}
		nextKey = stringutil.Bytes2str(recordResp.Kvs[l-1].Key)
		if i < minPage {
			// even through current page index less then the min page index,
			// but here must to get the nextKey and then continue
			continue
		}
		etcdResp.Kvs = append(etcdResp.Kvs, recordResp.Kvs[start:]...)
	}

	if op.Offset == -1 {
		c.logInfoOrWarn(start, fmt.Sprintf("get too many KeyValues(%s) from etcd, now paging.(%d vs %d)",
			key, recordCount, op.Limit))
	}

	// too slow
	if op.SortOrder == etcdadpt.SortDescend {
		t := time.Now()
		for i, l := 0, len(etcdResp.Kvs); i < l; i++ {
			last := l - i - 1
			if last <= i {
				break
			}
			etcdResp.Kvs[i], etcdResp.Kvs[last] = etcdResp.Kvs[last], etcdResp.Kvs[i]
		}
		c.logNilOrWarn(t, fmt.Sprintf("sorted descend %d KeyValues(%s)", recordCount, key))
	}
	return etcdResp, nil
}
