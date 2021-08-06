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

package metrics

import (
	"strings"

	"github.com/go-chassis/go-chassis/v2/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var PXX = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}

func mustRegister(name string, vec prometheus.Collector) {
	err := metrics.GetSystemPrometheusRegistry().Register(vec)
	if err != nil {
		panic(err)
	}
}

func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	name := strings.Join([]string{opts.Subsystem, opts.Name}, "_")
	vec := prometheus.NewCounterVec(opts, labelNames)
	mustRegister(name, vec)
	return vec
}

func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	name := strings.Join([]string{opts.Subsystem, opts.Name}, "_")
	vec := prometheus.NewGaugeVec(opts, labelNames)
	mustRegister(name, vec)
	return vec
}

func NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *prometheus.SummaryVec {
	name := strings.Join([]string{opts.Subsystem, opts.Name}, "_")
	vec := prometheus.NewSummaryVec(opts, labelNames)
	mustRegister(name, vec)
	return vec
}
