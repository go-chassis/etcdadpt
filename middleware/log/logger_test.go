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
package log_test

import (
	"testing"

	"github.com/go-chassis/etcdadpt/middleware/log"
	"github.com/go-chassis/openlog"
	"github.com/stretchr/testify/assert"
)

type mockLogger struct {
}

func (m *mockLogger) Debug(message string, opts ...openlog.Option) {
	panic("implement me")
}
func (m *mockLogger) Info(message string, opts ...openlog.Option) {
	panic("implement me")
}
func (m *mockLogger) Warn(message string, opts ...openlog.Option) {
	panic("implement me")
}
func (m *mockLogger) Error(message string, opts ...openlog.Option) {
	panic("implement me")
}
func (m *mockLogger) Fatal(message string, opts ...openlog.Option) {
	panic("implement me")
}

func TestGetLogger(t *testing.T) {
	assert.Equal(t, log.GetLogger(), openlog.GetLogger())

	newLogger := &mockLogger{}
	oldLogger := openlog.GetLogger()
	log.SetLogger(newLogger)
	defer log.SetLogger(oldLogger)
	assert.Equal(t, log.GetLogger(), newLogger)
}
