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

	"github.com/go-chassis/foundation/stringutil"
	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/metrics"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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

		if op.LargeRequestPaging() {
			etcdResp, err = c.LargeRequestPaging(ctx, op)
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
			Kvs:       etcdResp.Kvs,
			Count:     etcdResp.Count,
			Revision:  etcdResp.Header.Revision,
			Succeeded: true,
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
			Revision:  etcdResp.Header.Revision,
			Succeeded: true,
		}
	case etcdadpt.ActionDelete:
		var etcdResp *clientv3.DeleteResponse
		etcdResp, err = c.Client.Delete(otCtx, stringutil.Bytes2str(op.Key), c.toDeleteRequest(op)...)
		if err != nil {
			break
		}
		resp = &etcdadpt.Response{
			Revision:  etcdResp.Header.Revision,
			Succeeded: etcdResp.Deleted > 0,
		}
	}

	if err != nil {
		return nil, err
	}

	c.logNilOrWarn(start, fmt.Sprintf("registry client do %s", op))
	return resp, nil
}

func (c *Client) LargeRequestPaging(ctx context.Context, op etcdadpt.OpOptions) (*clientv3.GetResponse, error) {
	var etcdResp *clientv3.GetResponse
	key := stringutil.Bytes2str(op.Key)
	start := time.Now()

	countResp, err := c.Client.Get(ctx, key, append(c.toGetRequest(op), clientv3.WithCountOnly())...)
	if err != nil {
		return nil, err
	}

	recordCount := countResp.Count
	offset := op.Offset
	pageSize := op.Limit
	if pageSize == 0 {
		pageSize = etcdadpt.DefaultPageCount
	}
	if offset < 0 && recordCount <= pageSize {
		return nil, nil // no need to do paging
	}

	etcdResp = countResp
	etcdResp.Kvs = make([]*mvccpb.KeyValue, 0, etcdResp.Count)

	if offset >= recordCount {
		return etcdResp, nil
	}

	order := op.SortOrder
	begin, end, minPage, maxPage := getPageRange(recordCount, pageSize, offset, order)
	baseOps := c.toPagingOps(op, key, countResp.Header.Revision)
	nextKey := key
	for i := int64(0); i < maxPage; i++ {
		// get pageSize+1 records for the last key of the result is used as the first key of next page
		ops := append(baseOps, clientv3.WithLimit(pageSize+1))
		if i < minPage {
			// for the performance, just get the list without values
			ops = append(ops, clientv3.WithKeysOnly())
		}
		recordResp, err := c.Client.Get(ctx, nextKey, ops...)
		if err != nil {
			return nil, err
		}
		beginIndex := int64(0)
		endIndex := int64(len(recordResp.Kvs))
		if endIndex == 0 { // no more data, data may decrease during paging
			break
		}
		if endIndex > pageSize { // have a next page
			endIndex--
			nextKey = stringutil.Bytes2str(recordResp.Kvs[endIndex].Key)
		}
		if i < minPage {
			// even through current page index less then the min page index,
			// but here must to get the nextKey and then continue
			continue
		}
		if begin > 0 && i == minPage {
			beginIndex = begin
		}
		if end > 0 && i == maxPage-1 {
			endIndex = end
		}
		etcdResp.Kvs = append(etcdResp.Kvs, recordResp.Kvs[beginIndex:endIndex]...)
	}

	if offset < 0 {
		c.logInfoOrWarn(start, fmt.Sprintf("get too many KeyValues(%s) from etcd, now paging.(%d vs %d)",
			key, recordCount, pageSize))
	}

	// too slow
	if order == etcdadpt.SortDescend {
		c.reverseResult(etcdResp, recordCount, key)
	}
	return etcdResp, nil
}

func (c *Client) reverseResult(etcdResp *clientv3.GetResponse, recordCount int64, key string) {
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

func (c *Client) toPagingOps(op etcdadpt.OpOptions, key string, rev int64) []clientv3.OpOption {
	var baseOps []clientv3.OpOption
	tempOp := op
	tempOp.CountOnly = false
	tempOp.Prefix = false
	tempOp.SortOrder = etcdadpt.SortAscend
	if len(tempOp.EndKey) == 0 {
		tempOp.EndKey = []byte(clientv3.GetPrefixRangeEnd(key))
	}
	tempOp.Revision = rev
	baseOps = append(baseOps, c.toGetRequest(tempOp)...)
	return baseOps
}

func getPageRange(recordCount int64, pageSize int64, offset int64, order etcdadpt.SortOrder) (int64, int64, int64, int64) {
	pageCount := recordCount / pageSize
	remainCount := recordCount % pageSize
	if remainCount > 0 {
		pageCount++
	}
	begin, end, minPage, maxPage := int64(0), int64(0), int64(0), pageCount
	if offset < 0 {
		return 0, 0, minPage, maxPage
	}

	if order == etcdadpt.SortDescend {
		// if reverse order, to convert in ascend ordered offset
		offset = recordCount - offset - pageSize
	}
	count := offset + 1
	maxPage = count / pageSize
	begin = count % pageSize
	if begin > 1 {
		minPage = maxPage
		maxPage++
	} else if begin == 1 {
		// means the first record in one page
		minPage = maxPage
	} else {
		minPage = maxPage - 1
		begin = pageSize
	}
	begin--
	end = begin
	if maxPage >= pageCount {
		maxPage = pageCount
		end = 0
	} else {
		maxPage++
	}
	return begin, end, minPage, maxPage
}
