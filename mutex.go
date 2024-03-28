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
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-chassis/foundation/gopool"
	"github.com/go-chassis/openlog"

	"github.com/go-chassis/etcdadpt/middleware/log"
	"github.com/go-chassis/etcdadpt/middleware/metrics"
)

const (
	DefaultLockTTL    = 60
	DefaultRetryTimes = 3

	DefaultLock         = "/lock"
	OperationGlobalLock = "GLOBAL_LOCK"
)

var ErrLeaseIDNotExists = errors.New("leaseID is nil")
var ErrLockKeyFail = errors.New("fail to lock key")

type DLock struct {
	key      string
	ctx      context.Context
	ttl      int64
	mutex    *sync.Mutex
	id       string
	createAt time.Time
	leaseID  int64
}

var (
	IsDebug  bool
	hostname = getHostName()
	pid      = os.Getpid()
)

func newDLock(key string, ttl int64, wait bool) (*DLock, error) {
	var err error
	if len(key) == 0 {
		return nil, nil
	}
	if ttl < 1 {
		ttl = DefaultLockTTL
	}

	now := time.Now()
	l := &DLock{
		key:      key,
		ctx:      context.Background(),
		ttl:      ttl,
		id:       fmt.Sprintf("%v-%v-%v", hostname, pid, now.Format("20060102-15:04:05.999999999")),
		createAt: now,
		mutex:    &sync.Mutex{},
	}
	for try := 1; try <= DefaultRetryTimes; try++ {
		err = l.lock(wait)
		if err == nil {
			return l, err
		}

		if !wait {
			break
		}
	}
	log.GetLogger().Error(fmt.Sprintf("lock key %s failed, id=%s", l.key, l.id), openlog.WithErr(err))
	return nil, err
}

func (m *DLock) ID() string {
	return m.id
}

func (m *DLock) lock(wait bool) (err error) {
	if !IsDebug {
		m.mutex.Lock()
	}
	log.GetLogger().Info(fmt.Sprintf("trying to create a lock: key=%s, id=%s", m.key, m.id))
	var leaseID int64
	var opts []OpOption
	if m.ttl > 0 {
		leaseID, err = Instance().LeaseGrant(m.ctx, m.ttl)
		if err != nil {
			return err
		}
		opts = append(opts, WithLease(leaseID))
	}
	success, err := Insert(m.ctx, m.key, m.id, opts...)
	if err == nil && success {
		m.leaseID = leaseID
		log.GetLogger().Info(fmt.Sprintf("succeed to create lock, key=%s, id=%s", m.key, m.id))
		return nil
	}

	if leaseID > 0 {
		err = Instance().LeaseRevoke(m.ctx, leaseID)
		if err != nil {
			return err
		}
	}

	if m.ttl == 0 || !wait {
		return fmt.Errorf("err: %w ,key %s is locked by id=%s", ErrLockKeyFail, m.key, m.id)
	}

	log.GetLogger().Error(fmt.Sprintf("key %s is locked, waiting for other node releases it, id=%s", m.key, m.id), openlog.WithErr(err))

	ctx, cancel := context.WithTimeout(m.ctx, time.Duration(m.ttl)*time.Second)
	gopool.Go(func(context.Context) {
		defer cancel()
		err := Instance().Watch(ctx,
			WithStrKey(m.key),
			WithWatchCallback(
				func(message string, evt *Response) error {
					if evt != nil && evt.Action == ActionDelete {
						// break this for-loop, and try to create the node again.
						return fmt.Errorf("lock released")
					}
					return nil
				}))
		if err != nil {
			log.GetLogger().Warn(fmt.Sprintf("%s, key=%s, id=%s", err.Error(), m.key, m.id))
		}
	})
	select {
	case <-ctx.Done():
		return ctx.Err() // 可以重新尝试获取锁
	case <-m.ctx.Done():
		cancel()
		return m.ctx.Err() // 机制错误，不应该超时的
	}
}

func (m *DLock) Refresh() error {
	if m.leaseID != 0 {
		_, err := Instance().LeaseRenew(m.ctx, m.leaseID)
		return err
	}
	return ErrLeaseIDNotExists
}

func (m *DLock) Unlock() (err error) {
	defer func() {
		if !IsDebug {
			m.mutex.Unlock()
		}

		metrics.ReportBackendOperationCompleted(OperationGlobalLock, nil, m.createAt)
	}()

	for i := 1; i <= DefaultRetryTimes; i++ {
		_, err := Delete(m.ctx, m.key)
		if err == nil {
			log.GetLogger().Info(fmt.Sprintf("delete lock OK, key=%s, id=%s", m.key, m.id))
			return nil
		}
		log.GetLogger().Error(fmt.Sprintf("delete lock failed, key=%s, id=%s", m.key, m.id), openlog.WithErr(err))
	}
	return err
}

func getHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "UNKNOWN"
	}
	return hostname
}
