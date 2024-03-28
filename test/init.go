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

package test

import (
	"os"

	_ "github.com/go-chassis/etcdadpt/embedded"
	_ "github.com/go-chassis/etcdadpt/remote"

	"github.com/go-chassis/etcdadpt"
)

func init() {
	kind := os.Getenv("TEST_DB_KIND")
	uri := os.Getenv("TEST_DB_URI")
	if len(kind) == 0 {
		kind = "etcd"
	}
	if len(uri) == 0 && kind == "etcd" {
		uri = "127.0.0.1:2379"
	}
	etcdadpt.Init(etcdadpt.Config{
		Kind:             kind,
		ClusterAddresses: uri,
	})
}
