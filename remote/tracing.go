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

package remote

import (
	"context"
	"net/http"

	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/tracing"
)

func TracingBegin(ctx context.Context, operationName string, op etcdadpt.OpOptions) interface{} {
	return tracing.Begin(operationName, &tracing.Request{
		Ctx:      ctx,
		Endpoint: FirstEndpoint,
		Options:  op,
	})
}

func TracingEnd(span interface{}, err error) {
	if err != nil {
		tracing.End(span, &tracing.Response{Code: http.StatusInternalServerError, Message: err.Error()})
		return
	}
	tracing.End(span, &tracing.Response{Code: http.StatusOK})
}
