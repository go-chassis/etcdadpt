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

package main

import (
	"context"
	"fmt"

	_ "github.com/little-cui/etcdadpt/embedded"
	_ "github.com/little-cui/etcdadpt/remote"

	"github.com/go-chassis/openlog"
	"github.com/little-cui/etcdadpt"
)

func main() {
	err := SwitchToEmbeddedEtcdMode()
	if err != nil {
		openlog.Error(err.Error())
		return
	}
	DemoRun()

	err = SwitchToRemoteEtcdMode()
	if err != nil {
		openlog.Error(err.Error())
		return
	}
	DemoRun()
}

func DemoRun() {
	err := etcdadpt.Put(context.Background(), "/key/abc", "abc")
	if err != nil {
		openlog.Error(err.Error())
		return
	}
	defer func() {
		resp, err := etcdadpt.ListAndDelete(context.Background(), "/key/", etcdadpt.WithPrefix())
		if err != nil {
			openlog.Error(err.Error())
			return
		}
		openlog.Info(fmt.Sprintf("[DEMO] deleted: %s", resp))
	}()

	kv, err := etcdadpt.Get(context.Background(), "/key/abc")
	if err != nil {
		openlog.Error(err.Error())
		return
	}
	openlog.Info(fmt.Sprintf("[DEMO] put and get: %+v", kv))

	status, err := etcdadpt.Instance().Status(context.Background())
	if err != nil {
		openlog.Error(err.Error())
		return
	}
	openlog.Info(fmt.Sprintf("[DEMO] get status: %+v", status))
}

func SwitchToEmbeddedEtcdMode() error {
	openlog.Info("Switch to embedded etcd mode !!!")
	return etcdadpt.Init(etcdadpt.Config{
		Kind:             "embedded_etcd",
		ClusterAddresses: "default=http://127.0.0.1:2379",
	})
}

func SwitchToRemoteEtcdMode() error {
	openlog.Info("Switch to remote etcd mode !!!")
	return etcdadpt.Init(etcdadpt.Config{
		Kind:             "etcd",
		ClusterAddresses: "127.0.0.1:2379",
	})
}
