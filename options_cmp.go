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

func cmpVer(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpVersion }
}
func cmpCreateRev(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpCreate }
}
func cmpModRev(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpMod }
}
func cmpVal(key []byte) CmpOption {
	return func(op *CmpOptions) { op.Key = key; op.Type = CmpValue }
}
func cmpStrVer(key string) CmpOption       { return cmpVer([]byte(key)) }
func cmpStrCreateRev(key string) CmpOption { return cmpCreateRev([]byte(key)) }
func cmpStrModRev(key string) CmpOption    { return cmpModRev([]byte(key)) }
func cmpStrVal(key string) CmpOption       { return cmpVal([]byte(key)) }
func opCmp(opt CmpOption, result CmpResult, v interface{}) (cmp CmpOptions) {
	opt(&cmp)
	cmp.Result = result
	cmp.Value = v
	return cmp
}

// utils
func If(opts ...CmpOptions) []CmpOptions {
	return opts
}
func ExistKey(key string) CmpOptions {
	return NotEqualVer(key, 0)
}
func NotExistKey(key string) CmpOptions {
	return EqualCreateRev(key, 0)
}
func EqualVer(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrVer(key), CmpEqual, v)
}
func NotEqualVer(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrVer(key), CmpNotEqual, v)
}
func EqualVal(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrVal(key), CmpEqual, v)
}
func NotEqualVal(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrVal(key), CmpNotEqual, v)
}
func EqualCreateRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrCreateRev(key), CmpEqual, v)
}
func NotEqualCreateRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrCreateRev(key), CmpNotEqual, v)
}
func GreaterCreateRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrCreateRev(key), CmpGreater, v)
}
func LessCreateRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrCreateRev(key), CmpLess, v)
}
func EqualModRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrModRev(key), CmpEqual, v)
}
func NotEqualModRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrModRev(key), CmpNotEqual, v)
}
func GreaterModRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrModRev(key), CmpGreater, v)
}
func LessModRev(key string, v interface{}) CmpOptions {
	return opCmp(cmpStrModRev(key), CmpLess, v)
}
