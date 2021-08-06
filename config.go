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
	"crypto/tls"
	"time"

	"github.com/go-chassis/openlog"
)

const (
	// MaxTxnNumberOneTime the same as v3rpc.MaxOpsPerTxn = 128
	MaxTxnNumberOneTime = 128
	// DefaultPageCount grpc does not allow to transport a large body more then 4MB in a request
	DefaultPageCount = 4096
	// DefaultDialTimeout the timeout dial to etcd
	DefaultDialTimeout     = 10 * time.Second
	DefaultRequestTimeout  = 30 * time.Second
	DefaultCompactInterval = time.Hour
	DefaultClusterName     = "default"
)

type Config struct {
	// Kind plugin kind, can be 'etcd' or 'embedded_etcd'
	Kind string `json:"-"`
	// Logger logger for adapter, by default use openlog.GetLogger()
	Logger     openlog.Logger `json:"-"`
	SslEnabled bool           `json:"-"`
	TLSConfig  *tls.Config    `json:"-"`
	// ErrorFunc called when connection error occurs
	ErrorFunc func(err error) `json:"-"`
	// ConnectedFunc called when connected
	ConnectedFunc func() `json:"-"`
	// ManagerAddress optional, the list of cluster manager endpoints
	ManagerAddress string `json:"manageAddress,omitempty"`
	// ClusterName required when Kind = 'embedded_etcd'
	ClusterName string `json:"manageName,omitempty"`
	// ClusterAddresses required, the list of cluster client endpoints
	ClusterAddresses string        `json:"manageClusters,omitempty"` // the raw string of cluster configuration
	DialTimeout      time.Duration `json:"connectTimeout"`
	RequestTimeOut   time.Duration `json:"registryTimeout"`
	// AutoSyncInterval optional, then duration of auto sync the cluster members and check them health
	AutoSyncInterval time.Duration `json:"autoSyncInterval"`
	// CompactInterval optional, set DefaultCompactInterval if value equal to 0
	CompactInterval   time.Duration `json:"-"`
	CompactIndexDelta int64         `json:"-"`
}

func (c *Config) Init() {
	if c.Logger == nil {
		c.Logger = openlog.GetLogger()
	}
	if len(c.ClusterName) == 0 {
		c.ClusterName = DefaultClusterName
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.RequestTimeOut == 0 {
		c.RequestTimeOut = DefaultRequestTimeout
	}
	if c.CompactInterval == 0 {
		c.CompactInterval = DefaultCompactInterval
	}
}
