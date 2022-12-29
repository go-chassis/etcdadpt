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
	"errors"
	"strconv"
	"sync"
	"testing"

	_ "github.com/little-cui/etcdadpt/test"

	"github.com/little-cui/etcdadpt"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	t.Run("get not exist key, should return nil", func(t *testing.T) {
		kv, err := etcdadpt.Get(context.Background(), "/not-exist")
		assert.NoError(t, err)
		assert.Nil(t, kv)
	})
}

func TestList(t *testing.T) {
	var (
		resp *etcdadpt.Response
		err  error
	)

	t.Run("prepare data should be ok", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.PUT, etcdadpt.WithStrKey("/test_range/b"),
			etcdadpt.WithStrValue("b"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/b"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(1), resp.Count)
		assert.Equal(t, "/test_range/b", string(resp.Kvs[0].Key))
		assert.Equal(t, "b", string(resp.Kvs[0].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.PUT, etcdadpt.WithStrKey("/test_range/a"),
			etcdadpt.WithStrValue("a"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/a"),
			etcdadpt.WithKeyOnly())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(1), resp.Count)
		assert.Equal(t, "/test_range/a", string(resp.Kvs[0].Key))
		assert.Nil(t, resp.Kvs[0].Value)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/a"),
			etcdadpt.WithCountOnly())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(1), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.PUT, etcdadpt.WithStrKey("/test_range/d"),
			etcdadpt.WithStrValue("d"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.PUT, etcdadpt.WithStrKey("/test_range/c"),
			etcdadpt.WithStrValue("c"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.PUT, etcdadpt.WithStrKey("/test_range/dd"),
			etcdadpt.WithStrValue("dd"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
	})

	// get prefix
	t.Run("get prefix key should return ok", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/d"),
			etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(2), resp.Count)
		assert.Equal(t, "/test_range/d", string(resp.Kvs[0].Key))
		assert.Equal(t, "d", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[1].Key))
		assert.Equal(t, "dd", string(resp.Kvs[1].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/d"),
			etcdadpt.WithPrefix(), etcdadpt.WithKeyOnly())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(2), resp.Count)
		assert.Equal(t, "/test_range/d", string(resp.Kvs[0].Key))
		assert.Nil(t, resp.Kvs[0].Value)
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[1].Key))
		assert.Nil(t, resp.Kvs[1].Value)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/d"),
			etcdadpt.WithPrefix(), etcdadpt.WithCountOnly())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(2), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))
	})

	// get range
	t.Run("get range [b, dd) should not return a", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/b"),
			etcdadpt.WithStrEndKey("/test_range/dd")) // [b, dd) !!!
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(3), resp.Count)
		assert.Equal(t, "/test_range/b", string(resp.Kvs[0].Key))
		assert.Equal(t, "b", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[1].Key))
		assert.Equal(t, "c", string(resp.Kvs[1].Value))
		assert.Equal(t, "/test_range/d", string(resp.Kvs[2].Key))
		assert.Equal(t, "d", string(resp.Kvs[2].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/b"),
			etcdadpt.WithStrEndKey("/test_range/dd"), etcdadpt.WithKeyOnly()) // [b, dd) !!!
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(3), resp.Count)
		assert.Equal(t, "/test_range/b", string(resp.Kvs[0].Key))
		assert.Nil(t, resp.Kvs[0].Value)
		assert.Equal(t, "/test_range/c", string(resp.Kvs[1].Key))
		assert.Nil(t, resp.Kvs[1].Value)
		assert.Equal(t, "/test_range/d", string(resp.Kvs[2].Key))
		assert.Nil(t, resp.Kvs[2].Value)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/b"),
			etcdadpt.WithStrEndKey("/test_range/dd"), etcdadpt.WithCountOnly()) // [b, dd) !!!
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(3), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))
	})

	// get prefix paging a,b,c,d,dd
	t.Run("page 5 items limit 2 should return ok", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(0), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/a", string(resp.Kvs[0].Key))
		assert.Equal(t, "a", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/b", string(resp.Kvs[1].Key))
		assert.Equal(t, "b", string(resp.Kvs[1].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[0].Key))
		assert.Equal(t, "c", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/d", string(resp.Kvs[1].Key))
		assert.Equal(t, "d", string(resp.Kvs[1].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(4), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[0].Key))
		assert.Equal(t, "dd", string(resp.Kvs[0].Value))

		// custom offset
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(1), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/b", string(resp.Kvs[0].Key))
		assert.Equal(t, "b", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[1].Key))
		assert.Equal(t, "c", string(resp.Kvs[1].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(4), etcdadpt.WithLimit(3))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[0].Key))
		assert.Equal(t, "dd", string(resp.Kvs[0].Value))
	})

	t.Run("page 5 items limit 2 with key/count only should return ok", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(), etcdadpt.WithKeyOnly(),
			etcdadpt.WithOffset(4), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[0].Key))
		assert.Nil(t, resp.Kvs[0].Value)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/d"), etcdadpt.WithPrefix(), etcdadpt.WithKeyOnly(),
			etcdadpt.WithOffset(0), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(2), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/d", string(resp.Kvs[0].Key))
		assert.Nil(t, resp.Kvs[0].Value)
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[1].Key))
		assert.Nil(t, resp.Kvs[1].Value)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(), etcdadpt.WithCountOnly(),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))
	})

	t.Run("page 5 items limit 2 order by key desc should return c,b", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(0), etcdadpt.WithLimit(2), etcdadpt.WithDescendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[0].Key))
		assert.Equal(t, "dd", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/d", string(resp.Kvs[1].Key))
		assert.Equal(t, "d", string(resp.Kvs[1].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2), etcdadpt.WithDescendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[0].Key))
		assert.Equal(t, "c", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/b", string(resp.Kvs[1].Key))
		assert.Equal(t, "b", string(resp.Kvs[1].Value))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(4), etcdadpt.WithLimit(2), etcdadpt.WithDescendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, "/test_range/a", string(resp.Kvs[0].Key))
		assert.Equal(t, "a", string(resp.Kvs[0].Value))

		// custom offset
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(1), etcdadpt.WithLimit(2), etcdadpt.WithDescendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/d", string(resp.Kvs[0].Key))
		assert.Equal(t, "d", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[1].Key))
		assert.Equal(t, "c", string(resp.Kvs[1].Value))
	})
	/* TODO support it
	t.Run("page 2 limit 2 order by create rev should return d,c", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2), etcdadpt.WithOrderByCreate(), etcdadpt.WithAscendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/d", string(resp.Kvs[0].Key))
		assert.Equal(t, "d", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[1].Key))
		assert.Equal(t, "c", string(resp.Kvs[1].Value))
	})

	t.Run("page 3 limit 2 order by create rev should return dd", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(4), etcdadpt.WithLimit(2), etcdadpt.WithOrderByCreate(), etcdadpt.WithAscendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[0].Key))
		assert.Equal(t, "dd", string(resp.Kvs[0].Value))
	})

	t.Run("page 2 limit 2 order by create rev desc should return c,a", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2), etcdadpt.WithOrderByCreate(), etcdadpt.WithDescendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[0].Key))
		assert.Equal(t, "c", string(resp.Kvs[0].Value))
		assert.Equal(t, "/test_range/a", string(resp.Kvs[1].Key))
		assert.Equal(t, "a", string(resp.Kvs[1].Value))
	})

	t.Run("page 3 limit 2 order by create rev desc should return b", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(4), etcdadpt.WithLimit(2), etcdadpt.WithOrderByCreate(), etcdadpt.WithDescendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 1, len(resp.Kvs))
		assert.Equal(t, "/test_range/b", string(resp.Kvs[0].Key))
		assert.Equal(t, "b", string(resp.Kvs[0].Value))
	})
	*/

	t.Run("invalid offset should return empty", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(6), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))

		// if offset < -1, just paging by limit
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/"), etcdadpt.WithPrefix(),
			etcdadpt.WithOffset(-2), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(5), resp.Count)
		assert.Equal(t, 5, len(resp.Kvs))
	})

	// get range paging
	t.Run("get range and paging should return ok", func(t *testing.T) {
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/b"),
			etcdadpt.WithStrEndKey("/test_range/dd"),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(3), resp.Count)
		assert.Equal(t, 1, len(resp.Kvs))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/a"),
			etcdadpt.WithStrEndKey("/test_range/dd"),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(4), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[0].Key))
		assert.Equal(t, "/test_range/d", string(resp.Kvs[1].Key))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/a"),
			etcdadpt.WithStrEndKey("/test_range/dd"), etcdadpt.WithKeyOnly(),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(4), resp.Count)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/c", string(resp.Kvs[0].Key))
		assert.Nil(t, resp.Kvs[0].Value)
		assert.Equal(t, "/test_range/d", string(resp.Kvs[1].Key))
		assert.Nil(t, resp.Kvs[1].Value)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/a"),
			etcdadpt.WithStrEndKey("/test_range/dd"), etcdadpt.WithCountOnly(),
			etcdadpt.WithOffset(2), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(4), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/b"),
			etcdadpt.WithStrEndKey("/test_range/dd"),
			etcdadpt.WithOffset(5), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(3), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_range/a"),
			etcdadpt.WithStrEndKey("/test_range/dd"),
			etcdadpt.WithOffset(4), etcdadpt.WithLimit(2))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(4), resp.Count)
		assert.Equal(t, 0, len(resp.Kvs))
	})

	t.Run("delete kvs should return ok", func(t *testing.T) {
		// delete range
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.DEL,
			etcdadpt.WithStrKey("/test_range/b"),
			etcdadpt.WithStrEndKey("/test_range/dd")) // [b, d) !!!
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/"),
			etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, 2, len(resp.Kvs))
		assert.Equal(t, "/test_range/dd", string(resp.Kvs[1].Key))

		// delete prefix
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.DEL, etcdadpt.WithStrKey("/test_range/"),
			etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET, etcdadpt.WithStrKey("/test_range/"),
			etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(0), resp.Count)
	})

	t.Run("test large data should return ok", func(t *testing.T) {
		// large data
		var wg sync.WaitGroup
		for i := 0; i < etcdadpt.DefaultPageCount+1; i++ {
			wg.Add(1)
			v := strconv.Itoa(i)
			go func() {
				defer wg.Done()
				resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.PUT, etcdadpt.WithStrKey("/test_page/"+v),
					etcdadpt.WithStrValue(v))
				assert.NoError(t, err)
				assert.True(t, resp.Succeeded)
			}()
		}
		wg.Wait()
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_page/"),
			etcdadpt.WithStrEndKey("/test_page/9999"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(etcdadpt.DefaultPageCount+1), resp.Count)
		assert.Equal(t, etcdadpt.DefaultPageCount+1, len(resp.Kvs))

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_page/"), etcdadpt.WithPrefix(), etcdadpt.WithDescendOrder())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(etcdadpt.DefaultPageCount+1), resp.Count)
		assert.Equal(t, etcdadpt.DefaultPageCount+1, len(resp.Kvs))
		assert.Equal(t, "/test_page/999", string(resp.Kvs[0].Key))

		// delete range
		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.DEL,
			etcdadpt.WithStrKey("/test_page/"),
			etcdadpt.WithStrEndKey("/test_page/9999"))
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)

		resp, err = etcdadpt.Instance().Do(context.Background(), etcdadpt.GET,
			etcdadpt.WithStrKey("/test_page/"), etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.True(t, resp.Succeeded)
		assert.Equal(t, int64(0), resp.Count)
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
	t.Run("delete not exist key, should return false", func(t *testing.T) {
		del, err := etcdadpt.Delete(context.Background(), "not_exist_key")
		assert.NoError(t, err)
		assert.False(t, del)

		del, err = etcdadpt.Delete(context.Background(), "not_exist_key", etcdadpt.WithPrefix())
		assert.NoError(t, err)
		assert.False(t, del)
	})

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
		assert.False(t, del)
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

func TestLock(t *testing.T) {
	t.Run("lock key should pass", func(t *testing.T) {
		lock, err := etcdadpt.Lock("key", 5)
		assert.Nil(t, err)
		assert.NotNil(t, lock)
		t.Log("key locked")
		assert.NotNil(t, lock.ID())
		err = lock.Unlock()
		assert.Nil(t, err)
	})

	t.Run("key is not lock, using try lock should pass", func(t *testing.T) {
		lock, err := etcdadpt.TryLock("tryLock", 5)
		assert.Nil(t, err)
		assert.NotNil(t, lock)
		t.Log("tryLock locked")
		assert.NotNil(t, lock.ID())
		err = lock.Unlock()
		assert.Nil(t, err)
	})

	t.Run("key is lock, using try lock should fail", func(t *testing.T) {
		lock, err := etcdadpt.Lock("key1", 5)
		assert.Nil(t, err)
		assert.NotNil(t, lock)
		t.Log("key1 locked")
		tryLock, err := etcdadpt.TryLock("key1", 5)
		assert.Error(t, err)
		if !errors.Is(err, etcdadpt.ErrLockKeyFail) {
			t.Error(err)
		}
		assert.Nil(t, tryLock)
	})
}

func TestStatus(t *testing.T) {
	status, err := etcdadpt.Instance().Status(context.Background())
	assert.NoError(t, err)
	assert.NotZero(t, status.DBSize)
}
