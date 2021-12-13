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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/little-cui/etcdadpt"
	_ "github.com/little-cui/etcdadpt/test"
)

func init() {
	etcdadpt.IsDebug = true
}

func TestDLock(t *testing.T) {
	m1, err := etcdadpt.Lock("key1", 5)
	assert.NoError(t, err)
	assert.NotNil(t, m1)
	t.Log("m1 locked")

	ch := make(chan bool)
	go func() {
		m2, err := etcdadpt.TryLock("key1", 1)

		assert.Nil(t, m2)
		assert.Error(t, err)
		fmt.Println("m2 try failed")

		m2, err = etcdadpt.Lock("key1", 1)
		assert.Nil(t, m2)
		assert.Error(t, err)
		fmt.Println("m2 timed out")
		ch <- true
	}()
	<-ch

	m3, err := etcdadpt.Lock("key1", 2)
	assert.NoError(t, err)
	assert.NotNil(t, m3)

	fmt.Println("m3 locked")
	err = m3.Unlock()
	assert.NoError(t, err)

	err = m1.Unlock()
	assert.NoError(t, err)
	fmt.Println("m1 unlocked")
}

func TestLeaseRenew(t *testing.T) {

	t.Run("lock refreshKey should pass", func(t *testing.T) {
		refreshKey := "refreshKey"
		dLock, err := etcdadpt.Lock(refreshKey, 3)
		assert.NoError(t, err)
		assert.NotNil(t, dLock)
		t.Log("dLock locked")

		time.Sleep(1 * time.Second)

		err = dLock.Refresh()
		assert.Nil(t, err)

		m2, err := etcdadpt.TryLock(refreshKey, 1)
		assert.Nil(t, m2)
		assert.NotNil(t, err)
		if !errors.Is(err, etcdadpt.ErrLockKeyFail) {
			t.Error(err)
		}
		t.Log("m2 try failed")

		time.Sleep(5 * time.Second)
		m2, err = etcdadpt.TryLock(refreshKey, 1)
		assert.NotNil(t, m2)
		assert.Nil(t, err)

		err = dLock.Refresh()
		assert.NotNil(t, err)
	})
}
