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

package etcdadpt

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// Get get one kv
func Get(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	resp, err := Instance().Do(ctx, GET, WithStrKey(key))
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, nil
	}
	return resp.Kvs[0], err
}

func Insert(ctx context.Context, key, value string, opts ...OpOption) (bool, error) {
	op := OpPut(append(opts, WithStrKey(key), WithStrValue(value))...)
	return insert(ctx, op)
}

// InsertBytes insert a new kv, return false if the key exist
func InsertBytes(ctx context.Context, key string, value []byte, opts ...OpOption) (bool, error) {
	op := OpPut(append(opts, WithStrKey(key), WithValue(value))...)
	return insert(ctx, op)
}

func insert(ctx context.Context, op OpOptions) (bool, error) {
	resp, err := Instance().TxnWithCmp(ctx, Ops(op), If(NotExistKey(string(op.Key))), nil)
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

// Put insert or update kv
func Put(ctx context.Context, key string, value string, opts ...OpOption) error {
	opts = append(opts, PUT, WithStrKey(key), WithStrValue(value))
	_, err := Instance().Do(ctx, opts...)
	return err
}

// PutBytes insert or update kv
func PutBytes(ctx context.Context, key string, value []byte, opts ...OpOption) error {
	opts = append(opts, PUT, WithStrKey(key), WithValue(value))
	_, err := Instance().Do(ctx, opts...)
	return err
}

// PutBytesAndGet insert/update kv and return it
func PutBytesAndGet(ctx context.Context, key string, value []byte, opts ...OpOption) (*Response, error) {
	keyOp := WithStrKey(key)
	putOpts := append(opts, keyOp, WithValue(value))
	getOpts := append(opts, keyOp)
	return TxnWithCmp(ctx, Ops(OpPut(putOpts...), OpGet(getOpts...)), nil, nil)
}

// List get kv list
func List(ctx context.Context, key string, opts ...OpOption) ([]*mvccpb.KeyValue, int64, error) {
	opts = append(opts, GET, WithStrKey(key), WithPrefix())
	resp, err := Instance().Do(ctx, opts...)
	if err != nil {
		return nil, 0, err
	}
	return resp.Kvs, resp.Count, nil
}

// Exist get one kv, if can not get return false
func Exist(ctx context.Context, key string) (bool, error) {
	resp, err := Instance().Do(ctx, GET, WithStrKey(key), WithCountOnly())
	if err != nil {
		return false, err
	}
	if resp.Count == 0 {
		return false, nil
	}
	return true, nil
}

func Delete(ctx context.Context, key string, opts ...OpOption) (bool, error) {
	opts = append(opts, DEL, WithStrKey(key))
	resp, err := Instance().Do(ctx, opts...)
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func DeleteMany(ctx context.Context, opts ...OpOptions) (bool, error) {
	for _, opt := range opts {
		opt.Action = ActionDelete
	}
	err := Txn(ctx, opts)
	if err != nil {
		return false, err
	}
	return true, nil
}

// ListAndDelete delete key and return the deleted key
func ListAndDelete(ctx context.Context, key string, opts ...OpOption) (*Response, error) {
	keyOp := WithStrKey(key)
	listOpts := OpGet(append(opts, GET, keyOp)...)
	delOpts := OpDel(append(opts, DEL, keyOp)...)
	return TxnWithCmp(ctx, Ops(listOpts, delOpts), nil, nil)
}

// ListAndDeleteMany delete key and return the deleted key
func ListAndDeleteMany(ctx context.Context, opts ...OpOptions) (*Response, error) {
	var allOpts []OpOptions
	for _, opt := range opts {
		copyOpt := opt
		copyOpt.Action = ActionGet
		opt.Action = ActionDelete
		allOpts = append(allOpts, copyOpt, opt)
	}
	return TxnWithCmp(ctx, Ops(allOpts...), nil, nil)
}

func Txn(ctx context.Context, opts []OpOptions) error {
	_, err := TxnWithCmp(ctx, opts, nil, nil)
	return err
}

func TxnWithCmp(ctx context.Context, opts []OpOptions,
	cmp []CmpOptions, fail []OpOptions) (resp *Response, err error) {
	lenOpts := len(opts)
	tmpLen := lenOpts
	var tmpOpts []OpOptions
	for i := 0; tmpLen > 0; i++ {
		tmpLen = lenOpts - (i+1)*MaxTxnNumberOneTime
		if tmpLen > 0 {
			tmpOpts = opts[i*MaxTxnNumberOneTime : (i+1)*MaxTxnNumberOneTime]
		} else {
			tmpOpts = opts[i*MaxTxnNumberOneTime : lenOpts]
		}
		resp, err = Instance().TxnWithCmp(ctx, tmpOpts, cmp, fail)
		if err != nil || !resp.Succeeded {
			return
		}
	}
	return
}

func ListCluster(ctx context.Context) (Clusters, error) {
	return Instance().ListCluster(ctx)
}

// Lock func will lock the key, and retry three times if it fails.
// ttl unit is second.
func Lock(key string, ttl int64) (*DLock, error) {
	return newDLock(DefaultLock+"/"+key, ttl, true)
}

// TryLock func will try to lock the key.
// ttl unit is second.
func TryLock(key string, ttl int64) (*DLock, error) {
	return newDLock(DefaultLock+"/"+key, ttl, false)
}
