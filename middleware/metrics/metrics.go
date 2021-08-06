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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	success = "SUCCESS"
	failure = "FAILURE"
)

var (
	options          Options
	backendCounter   *prometheus.GaugeVec
	operationCounter *prometheus.CounterVec
	operationLatency *prometheus.SummaryVec
)

func Init(opts Options) {
	options = opts
	backendCounter = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: options.Namespace,
			Subsystem: "db",
			Name:      "backend_total",
			Help:      "Gauge of the backend instance",
		}, []string{"instance"})

	operationCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: options.Namespace,
			Subsystem: "db",
			Name:      "backend_operation_total",
			Help:      "Counter of backend operation",
		}, []string{"instance", "operation", "status"})

	operationLatency = NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  options.Namespace,
			Subsystem:  "db",
			Name:       "backend_operation_durations_microseconds",
			Help:       "Latency of backend operation",
			Objectives: PXX,
		}, []string{"instance", "operation", "status"})
}

func ReportBackendInstance(c int) {
	if backendCounter == nil {
		return
	}

	instance := options.InstanceName
	backendCounter.WithLabelValues(instance).Set(float64(c))
}

func ReportBackendOperationCompleted(operation string, err error, start time.Time) {
	if operationLatency == nil || operationCounter == nil {
		return
	}
	instance := options.InstanceName
	elapsed := float64(time.Since(start).Nanoseconds()) / float64(time.Microsecond)
	status := success
	if err != nil {
		status = failure
	}
	operationLatency.WithLabelValues(instance, operation, status).Observe(elapsed)
	operationCounter.WithLabelValues(instance, operation, status).Inc()
}
