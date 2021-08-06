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

package etcdadpt_test

import (
	"context"
	"testing"

	_ "github.com/little-cui/etcdadpt/remote"

	"github.com/little-cui/etcdadpt"
	"github.com/stretchr/testify/assert"
)

func init() {
	etcdadpt.Init(etcdadpt.Config{
		Kind:             "etcd",
		ClusterAddresses: "127.0.0.1:2379",
	})
}

func TestInsert(t *testing.T) {
	t.Run("put no override, should ok", func(t *testing.T) {
		success, err := etcdadpt.Insert(context.Background(), "/test_put_no_override/a", "a")
		assert.NoError(t, err)
		assert.True(t, success)

		success, err = etcdadpt.Insert(context.Background(), "/test_put_no_override/a", "changed")
		assert.NoError(t, err)
		assert.False(t, success)

		value, err := etcdadpt.Get(context.Background(), "/test_put_no_override/a")
		assert.NoError(t, err)
		assert.Equal(t, "a", string(value.Value))

		del, err := etcdadpt.Delete(context.Background(), "/test_put_no_override/a")
		assert.NoError(t, err)
		assert.True(t, del)

		exist, err := etcdadpt.Exist(context.Background(), "/test_put_no_override/a")
		assert.NoError(t, err)
		assert.False(t, exist)
	})
}

func TestGet(t *testing.T) {
	t.Run("get not exist key, should return nil", func(t *testing.T) {
		kv, err := etcdadpt.Get(context.Background(), "/not-exist")
		assert.NoError(t, err)
		assert.Nil(t, kv)
	})
}

func TestPut(t *testing.T) {
	t.Run("get and put kv, should return version 1", func(t *testing.T) {
		resp, err := etcdadpt.PutBytesAndGet(context.Background(), "/test_put_get/a", nil)
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(1), resp.Count)
		assert.Equal(t, int64(1), resp.Kvs[0].Version)

		del, err := etcdadpt.Delete(context.Background(), "/test_put_get/a")
		assert.NoError(t, err)
		assert.True(t, del)
	})
	t.Run("put kv, should return ok", func(t *testing.T) {
		err := etcdadpt.Put(context.Background(), "/test_put/a", "a")
		assert.NoError(t, err)

		kv, err := etcdadpt.Get(context.Background(), "/test_put/a")
		assert.NoError(t, err)
		assert.Equal(t, "a", string(kv.Value))

		del, err := etcdadpt.Delete(context.Background(), "/test_put/a")
		assert.NoError(t, err)
		assert.True(t, del)
	})
}

func TestTxnWithCmp(t *testing.T) {
	t.Run("put as update and not-exist key, should false", func(t *testing.T) {
		resp, err := etcdadpt.TxnWithCmp(context.Background(),
			etcdadpt.Ops(etcdadpt.OpPut(etcdadpt.WithStrKey("/test_update/a"), etcdadpt.WithStrValue("a"))),
			etcdadpt.If(etcdadpt.NotEqualVer("/test_update/a", 0)),
			nil)
		assert.NoError(t, err)
		assert.False(t, resp.Succeeded)
	})
	t.Run("put as insert, should false", func(t *testing.T) {
		resp, err := etcdadpt.TxnWithCmp(context.Background(),
			etcdadpt.Ops(etcdadpt.OpPut(etcdadpt.WithStrKey("/test_update/a"), etcdadpt.WithStrValue("a"))),
			etcdadpt.If(etcdadpt.EqualVer("/test_update/a", 0)),
			nil)
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
	})
	t.Run("put as update and key exist, should true", func(t *testing.T) {
		resp, err := etcdadpt.TxnWithCmp(context.Background(),
			etcdadpt.Ops(etcdadpt.OpPut(etcdadpt.WithStrKey("/test_update/a"), etcdadpt.WithStrValue("b"))),
			etcdadpt.If(etcdadpt.NotEqualVer("/test_update/a", 0)),
			nil)
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		kv, err := etcdadpt.Get(context.Background(), "/test_update/a")
		assert.NoError(t, err)
		assert.Equal(t, "b", string(kv.Value))

		del, err := etcdadpt.Delete(context.Background(), "/test_update/a")
		assert.NoError(t, err)
		assert.True(t, del)
	})
}

func TestDelete(t *testing.T) {
	t.Run("delete prefix, should return true", func(t *testing.T) {
		err := etcdadpt.Put(context.Background(), "/test_del_prefix/a", "a")
		assert.NoError(t, err)
		err = etcdadpt.Put(context.Background(), "/test_del_prefix/b", "b")
		assert.NoError(t, err)

		del, err := etcdadpt.Delete(context.Background(), "/test_del_prefix/", etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.True(t, del)

		kvs, n, err := etcdadpt.List(context.Background(), "/test_del_prefix/")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
		assert.Empty(t, kvs)

		del, err = etcdadpt.Delete(context.Background(), "/test_del_prefix/a")
		assert.NoError(t, err)
		assert.True(t, del)
	})

	t.Run("delete list, should return list", func(t *testing.T) {
		err := etcdadpt.Put(context.Background(), "/test_del_prefix/a", "a")
		assert.NoError(t, err)

		resp, err := etcdadpt.ListAndDelete(context.Background(), "/test_del_prefix/", etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, int64(1), resp.Count)

		kvs, n, err := etcdadpt.List(context.Background(), "/test_del_prefix/")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
		assert.Empty(t, kvs)

		resp, err = etcdadpt.ListAndDelete(context.Background(), "/test_del_prefix/a")
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, 0, len(resp.Kvs))
		assert.Equal(t, int64(0), resp.Count)
	})

	t.Run("delete many, should ok", func(t *testing.T) {
		err := etcdadpt.Put(context.Background(), "/test_del_many1/a", "a")
		assert.NoError(t, err)
		err = etcdadpt.Put(context.Background(), "/test_del_many2/b", "b")
		assert.NoError(t, err)

		del, err := etcdadpt.DeleteMany(context.Background(),
			etcdadpt.OpDel(etcdadpt.WithStrKey("/test_del_many1/a")),
			etcdadpt.OpDel(etcdadpt.WithStrKey("/test_del_many2/b")),
		)
		assert.NoError(t, err)
		assert.True(t, del)

		kvs, n, err := etcdadpt.List(context.Background(), "/test_del_many")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
		assert.Empty(t, kvs)
	})

	t.Run("list and delete many, should return list", func(t *testing.T) {
		err := etcdadpt.Put(context.Background(), "/test_del_many1/a", "a")
		assert.NoError(t, err)
		err = etcdadpt.Put(context.Background(), "/test_del_many2/b", "b")
		assert.NoError(t, err)

		resp, err := etcdadpt.ListAndDeleteMany(context.Background(),
			etcdadpt.OpDel(etcdadpt.WithStrKey("/test_del_many1/a")),
			etcdadpt.OpDel(etcdadpt.WithStrKey("/test_del_many2/b")),
		)
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, int64(2), resp.Count)

		kvs, n, err := etcdadpt.List(context.Background(), "/test_del_many")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
		assert.Empty(t, kvs)
	})
}
