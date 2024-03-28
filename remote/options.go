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
	"github.com/go-chassis/etcdadpt"
	"github.com/go-chassis/foundation/stringutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (c *Client) toGetRequest(op etcdadpt.OpOptions) []clientv3.OpOption {
	var opts []clientv3.OpOption
	if op.Prefix {
		opts = append(opts, clientv3.WithPrefix())
	} else if len(op.EndKey) > 0 {
		opts = append(opts, clientv3.WithRange(stringutil.Bytes2str(op.EndKey)))
	}
	if op.PrevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}
	if op.KeyOnly {
		opts = append(opts, clientv3.WithKeysOnly())
	}
	if op.CountOnly {
		opts = append(opts, clientv3.WithCountOnly())
	}
	if op.Revision > 0 {
		opts = append(opts, clientv3.WithRev(op.Revision))
	}
	// sort key by default and not need to set this flag
	sortTarget := clientv3.SortByKey
	switch op.OrderBy {
	case etcdadpt.OrderByCreate:
		sortTarget = clientv3.SortByCreateRevision
	case etcdadpt.OrderByMod:
		sortTarget = clientv3.SortByModRevision
	case etcdadpt.OrderByVer:
		sortTarget = clientv3.SortByVersion
	}
	switch op.SortOrder {
	case etcdadpt.SortAscend:
		opts = append(opts, clientv3.WithSort(sortTarget, clientv3.SortAscend))
	case etcdadpt.SortDescend:
		opts = append(opts, clientv3.WithSort(sortTarget, clientv3.SortDescend))
	}
	return opts
}

func (c *Client) toPutRequest(op etcdadpt.OpOptions) []clientv3.OpOption {
	var opts []clientv3.OpOption
	if op.PrevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}
	if op.Lease > 0 {
		opts = append(opts, clientv3.WithLease(clientv3.LeaseID(op.Lease)))
	}
	if op.IgnoreLease {
		opts = append(opts, clientv3.WithIgnoreLease())
	}
	return opts
}

func (c *Client) toDeleteRequest(op etcdadpt.OpOptions) []clientv3.OpOption {
	var opts []clientv3.OpOption
	if op.Prefix {
		opts = append(opts, clientv3.WithPrefix())
	} else if len(op.EndKey) > 0 {
		opts = append(opts, clientv3.WithRange(stringutil.Bytes2str(op.EndKey)))
	}
	if op.PrevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}
	return opts
}

func (c *Client) toTxnRequest(opts []etcdadpt.OpOptions) []clientv3.Op {
	var etcdOps []clientv3.Op
	for _, op := range opts {
		switch op.Action {
		case etcdadpt.ActionGet:
			etcdOps = append(etcdOps, clientv3.OpGet(stringutil.Bytes2str(op.Key), c.toGetRequest(op)...))
		case etcdadpt.ActionPut:
			var value string
			if len(op.Value) > 0 {
				value = stringutil.Bytes2str(op.Value)
			}
			etcdOps = append(etcdOps, clientv3.OpPut(stringutil.Bytes2str(op.Key), value, c.toPutRequest(op)...))
		case etcdadpt.ActionDelete:
			etcdOps = append(etcdOps, clientv3.OpDelete(stringutil.Bytes2str(op.Key), c.toDeleteRequest(op)...))
		}
	}
	return etcdOps
}

func (c *Client) toCompares(cmps []etcdadpt.CmpOptions) []clientv3.Cmp {
	var etcdCmps []clientv3.Cmp
	for _, cmp := range cmps {
		var cmpType clientv3.Cmp
		var cmpResult string
		key := stringutil.Bytes2str(cmp.Key)
		switch cmp.Type {
		case etcdadpt.CmpVersion:
			cmpType = clientv3.Version(key)
		case etcdadpt.CmpCreate:
			cmpType = clientv3.CreateRevision(key)
		case etcdadpt.CmpMod:
			cmpType = clientv3.ModRevision(key)
		case etcdadpt.CmpValue:
			cmpType = clientv3.Value(key)
		}
		switch cmp.Result {
		case etcdadpt.CmpEqual:
			cmpResult = "="
		case etcdadpt.CmpGreater:
			cmpResult = ">"
		case etcdadpt.CmpLess:
			cmpResult = "<"
		case etcdadpt.CmpNotEqual:
			cmpResult = "!="
		}
		etcdCmps = append(etcdCmps, clientv3.Compare(cmpType, cmpResult, cmp.Value))
	}
	return etcdCmps
}
