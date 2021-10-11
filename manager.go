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
	"errors"
	"fmt"
	"time"

	"github.com/go-chassis/foundation/backoff"
	"github.com/little-cui/etcdadpt/middleware/log"
	"google.golang.org/grpc/grpclog"
)

var (
	ErrNoPlugin = errors.New("required etcd adapter implement, please import the pkg")

	plugins    = make(map[string]newClientFunc)
	pluginInst Client
)

type newClientFunc func(cfg Config) Client

// Install load plugins configuration into plugins
func Install(pluginImplName string, newFunc newClientFunc) {
	plugins[pluginImplName] = newFunc
}

// Init construct storage plugin instance
// invoked by sc main process
func Init(cfg Config) error {
	cfg.Init()

	grpclog.SetLoggerV2(&log.Logger{Logger: cfg.Logger})

	for i := 0; ; i++ {
		inst, err := NewInstance(cfg)
		if err != nil {
			cfg.Logger.Error(fmt.Sprintf("init etcd failed, error: %s", err))
			if err == ErrNoPlugin {
				return err
			}
		} else {
			pluginInst = inst
			break
		}

		<-time.After(backoff.GetBackoff().Delay(i))
	}
	return nil
}

func NewInstance(cfg Config) (Client, error) {
	if cfg.Kind == "" {
		return nil, ErrNoPlugin
	}

	f, ok := plugins[cfg.Kind]
	if !ok {
		return nil, ErrNoPlugin
	}
	inst := f(cfg)
	select {
	case err := <-inst.Err():
		return nil, err
	case <-inst.Ready():
		return inst, nil
	}
}

// Instance is the instance of Etcd client
func Instance() Client {
	if pluginInst != nil {
		<-pluginInst.Ready()
	}
	return pluginInst
}
