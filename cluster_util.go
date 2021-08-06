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
	"strings"
)

// ParseClusters convert the cluster url string to Clusters type.
// The clusterURLs format like 'sc-0=http(s)://host1:port1,http(s)://host2:port2,sc-1=http(s)://host3:port3',
// managerURLs is optional, set the result value with the key clusterName
func ParseClusters(clusterName, clusterURLs, managerURLs string) Clusters {
	clusters := make(Clusters)
	kvs := strings.Split(clusterURLs, "=")
	if l := len(kvs); l >= 2 {
		var (
			names []string
			addrs [][]string
		)
		for i := 0; i < l; i++ {
			ss := strings.Split(kvs[i], ",")
			sl := len(ss)
			if i != l-1 {
				names = append(names, ss[sl-1])
			}
			if i != 0 {
				if sl > 1 && i != l-1 {
					addrs = append(addrs, ss[:sl-1])
				} else {
					addrs = append(addrs, ss)
				}
			}
		}
		for i, name := range names {
			clusters[name] = addrs[i]
		}
		if len(managerURLs) > 0 {
			clusters[clusterName] = strings.Split(managerURLs, ",")
		}
	}
	if len(clusters) == 0 {
		clusters[clusterName] = strings.Split(clusterURLs, ",")
	}
	return clusters
}

func GetClusterURL(clusterName, clusterURLs, managerURLs string) []string {
	clusters := ParseClusters(clusterName, clusterURLs, managerURLs)
	return clusters[clusterName]
}
