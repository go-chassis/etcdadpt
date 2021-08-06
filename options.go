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
	"bytes"
	"fmt"
)

type OpOptions struct {
	Action        Action
	Key           []byte
	EndKey        []byte
	Value         []byte
	Prefix        bool
	PrevKV        bool
	Lease         int64
	KeyOnly       bool
	CountOnly     bool
	OrderBy       SortTarget
	SortOrder     SortOrder
	Revision      int64
	IgnoreLease   bool
	Mode          CacheMode
	WatchCallback WatchCallback
	Offset        int64
	Limit         int64
	Global        bool
}

func (op OpOptions) String() string {
	return op.URI()
}

func (op OpOptions) URI() string {
	var buf bytes.Buffer
	buf.WriteString("action=")
	buf.WriteString(op.Action.String())
	buf.WriteString("&mode=")
	buf.WriteString(op.Mode.String())
	buf.WriteString("&key=")
	buf.Write(op.Key)
	buf.WriteString(fmt.Sprintf("&len=%d", len(op.Value)))
	if len(op.EndKey) > 0 {
		buf.WriteString("&end=")
		buf.Write(op.EndKey)
	}
	if op.Prefix {
		buf.WriteString("&prefix=true")
	}
	if op.PrevKV {
		buf.WriteString("&prev=true")
	}
	if op.Lease > 0 {
		buf.WriteString(fmt.Sprintf("&lease=%d", op.Lease))
	}
	if op.KeyOnly {
		buf.WriteString("&keyOnly=true")
	}
	if op.CountOnly {
		buf.WriteString("&countOnly=true")
	}
	if op.SortOrder != SortNone {
		buf.WriteString("&sort=")
		buf.WriteString(op.SortOrder.String())
		buf.WriteString("&orderBy=")
		buf.WriteString(op.OrderBy.String())
	}
	if op.Revision > 0 {
		buf.WriteString(fmt.Sprintf("&rev=%d", op.Revision))
	}
	if op.IgnoreLease {
		buf.WriteString("&ignoreLease=true")
	}
	if op.Offset > 0 {
		buf.WriteString(fmt.Sprintf("&offset=%d", op.Offset))
	}
	if op.Limit > 0 {
		buf.WriteString(fmt.Sprintf("&limit=%d", op.Limit))
	}
	if op.Global {
		buf.WriteString("&global=true")
	}
	return buf.String()
}

func (op OpOptions) NoCache() bool {
	return op.Mode == ModeNoCache ||
		op.Revision > 0 ||
		(op.Offset >= 0 && op.Limit > 0)
}

func (op OpOptions) CacheOnly() bool {
	return op.Mode == ModeCache
}

type OpOption func(*OpOptions)
type Operation func(...OpOption) (op OpOptions)
type WatchCallback func(message string, evt *Response) error

var GET OpOption = func(op *OpOptions) { op.Action = ActionGet }
var PUT OpOption = func(op *OpOptions) { op.Action = ActionPut }
var DEL OpOption = func(op *OpOptions) { op.Action = ActionDelete }

func WithKey(key []byte) OpOption      { return func(op *OpOptions) { op.Key = key } }
func WithEndKey(key []byte) OpOption   { return func(op *OpOptions) { op.EndKey = key } }
func WithValue(value []byte) OpOption  { return func(op *OpOptions) { op.Value = value } }
func WithPrefix() OpOption             { return func(op *OpOptions) { op.Prefix = true } }
func WithPrevKv() OpOption             { return func(op *OpOptions) { op.PrevKV = true } }
func WithLease(leaseID int64) OpOption { return func(op *OpOptions) { op.Lease = leaseID } }
func WithKeyOnly() OpOption            { return func(op *OpOptions) { op.KeyOnly = true } }
func WithCountOnly() OpOption          { return func(op *OpOptions) { op.CountOnly = true } }
func WithGlobal() OpOption             { return func(op *OpOptions) { op.Global = true } }
func WithOrderByCreate() OpOption      { return func(op *OpOptions) { op.OrderBy = OrderByCreate } }
func WithNoneOrder() OpOption          { return func(op *OpOptions) { op.SortOrder = SortNone } }
func WithAscendOrder() OpOption        { return func(op *OpOptions) { op.SortOrder = SortAscend } }
func WithDescendOrder() OpOption       { return func(op *OpOptions) { op.SortOrder = SortDescend } }
func WithRev(revision int64) OpOption  { return func(op *OpOptions) { op.Revision = revision } }
func WithIgnoreLease() OpOption        { return func(op *OpOptions) { op.IgnoreLease = true } }
func WithCacheOnly() OpOption          { return func(op *OpOptions) { op.Mode = ModeCache } }
func WithNoCache() OpOption            { return func(op *OpOptions) { op.Mode = ModeNoCache } }
func WithWatchCallback(f WatchCallback) OpOption {
	return func(op *OpOptions) { op.WatchCallback = f }
}
func WithStrKey(key string) OpOption     { return WithKey([]byte(key)) }
func WithStrEndKey(key string) OpOption  { return WithEndKey([]byte(key)) }
func WithStrValue(value string) OpOption { return WithValue([]byte(value)) }
func WithOffset(i int64) OpOption        { return func(op *OpOptions) { op.Offset = i } }
func WithLimit(i int64) OpOption         { return func(op *OpOptions) { op.Limit = i } }
func WatchPrefixOpOptions(key string) []OpOption {
	return []OpOption{GET, WithStrKey(key), WithPrefix(), WithPrevKv()}
}

func OpGet(opts ...OpOption) (op OpOptions) {
	op = OptionsToOp(opts...)
	op.Action = ActionGet
	return
}
func OpPut(opts ...OpOption) (op OpOptions) {
	op = OptionsToOp(opts...)
	op.Action = ActionPut
	return
}
func OpDel(opts ...OpOption) (op OpOptions) {
	op = OptionsToOp(opts...)
	op.Action = ActionDelete
	return
}
func OptionsToOp(opts ...OpOption) (op OpOptions) {
	for _, opt := range opts {
		opt(&op)
	}
	if op.Limit == 0 {
		op.Offset = -1
		op.Limit = DefaultPageCount
	}
	return
}

func Ops(ops ...OpOptions) []OpOptions {
	return ops
}
