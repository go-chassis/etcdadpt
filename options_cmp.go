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

import "fmt"

type CmpOptions struct {
	Key    []byte
	Type   CmpType
	Result CmpResult
	Value  interface{}
}

func (op CmpOptions) String() string {
	return fmt.Sprintf("{key: %s, type: %s, result: %s, val: %s}",
		op.Key, op.Type, op.Result, op.Value)
}

type CmpOption func(op *CmpOptions)

func CmpVer(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpVersion }
}
func CmpCreateRev(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpCreate }
}
func CmpModRev(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpMod }
}
func CmpVal(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpValue }
}
func CmpStrVer(key string) CmpOption       { return CmpVer([]byte(key)) }
func CmpStrCreateRev(key string) CmpOption { return CmpCreateRev([]byte(key)) }
func CmpStrModRev(key string) CmpOption    { return CmpModRev([]byte(key)) }
func CmpStrVal(key string) CmpOption       { return CmpVal([]byte(key)) }
func OpCmp(opt CmpOption, result CmpResult, v interface{}) (cmp CmpOptions) {
	opt(&cmp)
	cmp.Result = result
	cmp.Value = v
	return cmp
}

// utils
func If(opts ...CmpOptions) []CmpOptions {
	return opts
}
func EqualVer(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrVer(key), CmpEqual, v)
}
func NotEqualVer(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrVer(key), CmpNotEqual, v)
}
func EqualVal(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrVal(key), CmpEqual, v)
}
func NotEqualVal(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrVal(key), CmpNotEqual, v)
}
func EqualCreateRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrCreateRev(key), CmpEqual, v)
}
func NotEqualCreateRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrCreateRev(key), CmpNotEqual, v)
}
func GreaterCreateRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrCreateRev(key), CmpGreater, v)
}
func LessCreateRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrCreateRev(key), CmpLess, v)
}
func EqualModRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrModRev(key), CmpEqual, v)
}
func NotEqualModRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrModRev(key), CmpNotEqual, v)
}
func GreaterModRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrModRev(key), CmpGreater, v)
}
func LessModRev(key string, v interface{}) CmpOptions {
	return OpCmp(CmpStrModRev(key), CmpLess, v)
}
