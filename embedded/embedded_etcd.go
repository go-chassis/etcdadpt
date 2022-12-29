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

package embedded

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3compactor"
	"go.etcd.io/etcd/server/v3/lease"

	"github.com/go-chassis/foundation/gopool"
	"github.com/go-chassis/foundation/stringutil"
	"github.com/little-cui/etcdadpt"
	"github.com/little-cui/etcdadpt/middleware/log"
)

const DefaultDataDir = "data"

func init() {
	etcdadpt.Install("embeded_etcd", NewEmbeddedEtcd) //TODO remove misspell in future
	etcdadpt.Install("embedded_etcd", NewEmbeddedEtcd)
}

type EtcdEmbed struct {
	Cfg       etcdadpt.Config
	Embed     *embed.Etcd
	err       chan error
	ready     chan struct{}
	goroutine *gopool.Pool
}

func (s *EtcdEmbed) Err() <-chan error {
	return s.err
}

func (s *EtcdEmbed) Ready() <-chan struct{} {
	return s.ready
}

func (s *EtcdEmbed) Close() {
	if s.Embed != nil {
		s.Embed.Close()
	}
	s.goroutine.Close(true)
	log.GetLogger().Debug("embedded etcd client stopped")
}

func (s *EtcdEmbed) getPrefixEndKey(prefix []byte) []byte {
	l := len(prefix)
	endBytes := make([]byte, l+1)
	copy(endBytes, prefix)
	if endBytes[l-1] == 0xff {
		endBytes[l] = 1
		return endBytes
	}
	endBytes[l-1]++
	return endBytes[:l]
}

func (s *EtcdEmbed) toGetRequest(op etcdadpt.OpOptions) *etcdserverpb.RangeRequest {
	endBytes := op.EndKey
	if op.Prefix {
		endBytes = s.getPrefixEndKey(op.Key)
	}
	sortTarget := etcdserverpb.RangeRequest_KEY
	switch op.OrderBy {
	case etcdadpt.OrderByCreate:
		sortTarget = etcdserverpb.RangeRequest_CREATE
	case etcdadpt.OrderByMod:
		sortTarget = etcdserverpb.RangeRequest_MOD
	case etcdadpt.OrderByVer:
		sortTarget = etcdserverpb.RangeRequest_VERSION
	}
	order := etcdserverpb.RangeRequest_NONE
	switch op.SortOrder {
	case etcdadpt.SortAscend:
		order = etcdserverpb.RangeRequest_ASCEND
	case etcdadpt.SortDescend:
		order = etcdserverpb.RangeRequest_DESCEND
	}
	return &etcdserverpb.RangeRequest{
		Key:        op.Key,
		RangeEnd:   endBytes,
		KeysOnly:   op.KeyOnly,
		CountOnly:  op.CountOnly,
		SortOrder:  order,
		SortTarget: sortTarget,
		Revision:   op.Revision,
	}
}

func (s *EtcdEmbed) toPutRequest(op etcdadpt.OpOptions) *etcdserverpb.PutRequest {
	var valueBytes []byte
	if len(op.Value) > 0 {
		valueBytes = op.Value
	}
	return &etcdserverpb.PutRequest{
		Key:         op.Key,
		Value:       valueBytes,
		PrevKv:      op.PrevKV,
		Lease:       op.Lease,
		IgnoreLease: op.IgnoreLease,
	}
}

func (s *EtcdEmbed) toDeleteRequest(op etcdadpt.OpOptions) *etcdserverpb.DeleteRangeRequest {
	endBytes := op.EndKey
	if op.Prefix {
		endBytes = s.getPrefixEndKey(op.Key)
	}
	return &etcdserverpb.DeleteRangeRequest{
		Key:      op.Key,
		RangeEnd: endBytes,
		PrevKv:   op.PrevKV,
	}
}

func (s *EtcdEmbed) toTxnRequest(opts []etcdadpt.OpOptions) []*etcdserverpb.RequestOp {
	etcdOps := []*etcdserverpb.RequestOp{}
	for _, op := range opts {
		switch op.Action {
		case etcdadpt.ActionGet:
			etcdOps = append(etcdOps, &etcdserverpb.RequestOp{
				Request: &etcdserverpb.RequestOp_RequestRange{
					RequestRange: s.toGetRequest(op),
				},
			})
		case etcdadpt.ActionPut:
			etcdOps = append(etcdOps, &etcdserverpb.RequestOp{
				Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: s.toPutRequest(op),
				},
			})
		case etcdadpt.ActionDelete:
			etcdOps = append(etcdOps, &etcdserverpb.RequestOp{
				Request: &etcdserverpb.RequestOp_RequestDeleteRange{
					RequestDeleteRange: s.toDeleteRequest(op),
				},
			})
		}
	}
	return etcdOps
}

func (s *EtcdEmbed) toCompares(cmps []etcdadpt.CmpOptions) []*etcdserverpb.Compare {
	etcdCmps := []*etcdserverpb.Compare{}
	for _, cmp := range cmps {
		compare := &etcdserverpb.Compare{
			Key: cmp.Key,
		}
		switch cmp.Type {
		case etcdadpt.CmpVersion:
			var version int64
			if cmp.Value != nil {
				if v, ok := cmp.Value.(int64); ok {
					version = v
				}
			}
			compare.Target = etcdserverpb.Compare_VERSION
			compare.TargetUnion = &etcdserverpb.Compare_Version{
				Version: version,
			}
		case etcdadpt.CmpCreate:
			var revision int64
			if cmp.Value != nil {
				if v, ok := cmp.Value.(int64); ok {
					revision = v
				}
			}
			compare.Target = etcdserverpb.Compare_CREATE
			compare.TargetUnion = &etcdserverpb.Compare_CreateRevision{
				CreateRevision: revision,
			}
		case etcdadpt.CmpMod:
			var revision int64
			if cmp.Value != nil {
				if v, ok := cmp.Value.(int64); ok {
					revision = v
				}
			}
			compare.Target = etcdserverpb.Compare_MOD
			compare.TargetUnion = &etcdserverpb.Compare_ModRevision{
				ModRevision: revision,
			}
		case etcdadpt.CmpValue:
			var value []byte
			if cmp.Value != nil {
				if v, ok := cmp.Value.([]byte); ok {
					value = v
				}
			}
			compare.Target = etcdserverpb.Compare_VALUE
			compare.TargetUnion = &etcdserverpb.Compare_Value{
				Value: value,
			}
		}
		switch cmp.Result {
		case etcdadpt.CmpEqual:
			compare.Result = etcdserverpb.Compare_EQUAL
		case etcdadpt.CmpGreater:
			compare.Result = etcdserverpb.Compare_GREATER
		case etcdadpt.CmpLess:
			compare.Result = etcdserverpb.Compare_LESS
		case etcdadpt.CmpNotEqual:
			compare.Result = etcdserverpb.Compare_NOT_EQUAL
		}
		etcdCmps = append(etcdCmps, compare)
	}
	return etcdCmps
}

func (s *EtcdEmbed) Compact(ctx context.Context, reserve int64) error {
	curRev := s.getLeaderCurrentRevision(ctx)
	revToCompact := max(0, curRev-reserve)
	if revToCompact <= 0 {
		log.GetLogger().Info(fmt.Sprintf("revision is %d, <=%d, no nead to compact", curRev, reserve))
		return nil
	}

	log.GetLogger().Info(fmt.Sprintf("compacting... revision is %d(current: %d, reserve %d)", revToCompact, curRev, reserve))
	_, err := s.Embed.Server.Compact(ctx, &etcdserverpb.CompactionRequest{
		Revision: revToCompact,
		Physical: true,
	})
	if err != nil {
		log.GetLogger().Error(fmt.Sprintf("compact locally failed, revision is %d(current: %d, reserve %d), error: %s",
			revToCompact, curRev, reserve, err))
		return err
	}
	log.GetLogger().Info(fmt.Sprintf("compacted locally, revision is %d(current: %d, reserve %d)", revToCompact, curRev, reserve))

	// TODO defragment
	log.GetLogger().Info("defraged locally")

	return nil
}

func (s *EtcdEmbed) getLeaderCurrentRevision(ctx context.Context) int64 {
	return s.Embed.Server.KV().Rev()
}

func (s *EtcdEmbed) Do(ctx context.Context, opts ...etcdadpt.OpOption) (*etcdadpt.Response, error) {
	op := etcdadpt.OptionsToOp(opts...)

	otCtx, cancel := s.WithTimeout(ctx)
	defer cancel()
	var err error
	var resp *etcdadpt.Response
	switch op.Action {
	case etcdadpt.ActionGet:
		// TODO large request paging
		var etcdResp *etcdserverpb.RangeResponse
		etcdResp, err = s.Embed.Server.Range(otCtx, s.toGetRequest(op))
		if err != nil {
			break
		}
		if op.LargeRequestPaging() && op.Offset >= 0 && op.Limit > 0 {
			pagingResult(op, etcdResp)
		}
		resp = &etcdadpt.Response{
			Kvs:       etcdResp.Kvs,
			Count:     etcdResp.Count,
			Revision:  etcdResp.Header.Revision,
			Succeeded: true,
		}
	case etcdadpt.ActionPut:
		var etcdResp *etcdserverpb.PutResponse
		etcdResp, err = s.Embed.Server.Put(otCtx, s.toPutRequest(op))
		if err != nil {
			break
		}
		resp = &etcdadpt.Response{
			Revision:  etcdResp.Header.Revision,
			Succeeded: true,
		}
	case etcdadpt.ActionDelete:
		var etcdResp *etcdserverpb.DeleteRangeResponse
		etcdResp, err = s.Embed.Server.DeleteRange(otCtx, s.toDeleteRequest(op))
		if err != nil {
			break
		}
		resp = &etcdadpt.Response{
			Revision:  etcdResp.Header.Revision,
			Succeeded: etcdResp.Deleted > 0,
		}
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func pagingResult(op etcdadpt.OpOptions, etcdResp *etcdserverpb.RangeResponse) {
	if op.Offset >= etcdResp.Count {
		etcdResp.Kvs = []*mvccpb.KeyValue{}
		return
	}
	end := op.Offset + op.Limit
	if end > etcdResp.Count {
		end = etcdResp.Count
	}
	etcdResp.Kvs = etcdResp.Kvs[op.Offset:end]
}

// TODO EMBED支持KV().TxnBegin()->TxnID，可惜PROXY模式暂时不支持
func (s *EtcdEmbed) Txn(ctx context.Context, opts []etcdadpt.OpOptions) (*etcdadpt.Response, error) {
	resp, err := s.TxnWithCmp(ctx, opts, nil, nil)
	if err != nil {
		return nil, err
	}
	return &etcdadpt.Response{
		Succeeded: resp.Succeeded,
		Revision:  resp.Revision,
	}, nil
}

func (s *EtcdEmbed) TxnWithCmp(ctx context.Context, success []etcdadpt.OpOptions, cmps []etcdadpt.CmpOptions, fail []etcdadpt.OpOptions) (*etcdadpt.Response, error) {
	otCtx, cancel := s.WithTimeout(ctx)
	defer cancel()

	etcdCmps := s.toCompares(cmps)
	etcdSuccessOps := s.toTxnRequest(success)
	etcdFailOps := s.toTxnRequest(fail)
	txnRequest := &etcdserverpb.TxnRequest{
		Success: etcdSuccessOps,
	}
	if len(etcdCmps) > 0 {
		txnRequest.Compare = etcdCmps
	}
	if len(etcdFailOps) > 0 {
		txnRequest.Failure = etcdFailOps
	}
	resp, err := s.Embed.Server.Txn(otCtx, txnRequest)
	if err != nil {
		if err.Error() == rpctypes.ErrKeyNotFound.Error() {
			// etcd return ErrKeyNotFound if key does not exist and
			// the PUT options contain WithIgnoreLease
			return &etcdadpt.Response{Succeeded: false}, nil
		}
		return nil, err
	}

	var rangeResponse etcdserverpb.RangeResponse
	for _, itf := range resp.Responses {
		if rr, ok := itf.Response.(*etcdserverpb.ResponseOp_ResponseRange); ok {
			// plz request the same type range kv in txn success/fail options
			rangeResponse.Kvs = append(rangeResponse.Kvs, rr.ResponseRange.Kvs...)
			rangeResponse.Count += rr.ResponseRange.Count
		}
	}

	return &etcdadpt.Response{
		Succeeded: resp.Succeeded,
		Revision:  resp.Header.Revision,
		Kvs:       rangeResponse.Kvs,
		Count:     rangeResponse.Count,
	}, nil
}

func (s *EtcdEmbed) LeaseGrant(ctx context.Context, TTL int64) (int64, error) {
	otCtx, cancel := s.WithTimeout(ctx)
	defer cancel()
	etcdResp, err := s.Embed.Server.LeaseGrant(otCtx, &etcdserverpb.LeaseGrantRequest{
		TTL: TTL,
	})
	if err != nil {
		return 0, err
	}
	return etcdResp.ID, nil
}

func (s *EtcdEmbed) LeaseRenew(ctx context.Context, leaseID int64) (int64, error) {
	otCtx, cancel := s.WithTimeout(ctx)
	defer cancel()
	ttl, err := s.Embed.Server.LeaseRenew(otCtx, lease.LeaseID(leaseID))
	if err != nil {
		if err.Error() == rpctypes.ErrLeaseNotFound.Error() {
			return 0, etcdadpt.ErrLeaseNotFound
		}
		return 0, err
	}
	return ttl, nil
}

func (s *EtcdEmbed) LeaseRevoke(ctx context.Context, leaseID int64) error {
	otCtx, cancel := s.WithTimeout(ctx)
	defer cancel()
	_, err := s.Embed.Server.LeaseRevoke(otCtx, &etcdserverpb.LeaseRevokeRequest{
		ID: leaseID,
	})
	if err != nil {
		if err.Error() == rpctypes.ErrLeaseNotFound.Error() {
			return etcdadpt.ErrLeaseNotFound
		}
		return err
	}
	return nil
}

func (s *EtcdEmbed) Watch(ctx context.Context, opts ...etcdadpt.OpOption) (err error) {
	op := etcdadpt.OpGet(opts...)

	if len(op.Key) > 0 {
		watchable := s.Embed.Server.Watchable()
		ws := watchable.NewWatchStream()
		defer ws.Close()

		key := stringutil.Bytes2str(op.Key)
		var keyBytes []byte
		if op.Prefix {
			if key[len(key)-1] != '/' {
				key += "/"
			}
			keyBytes = s.getPrefixEndKey(stringutil.Str2bytes(key))
		}
		watchID, err := ws.Watch(0, op.Key, keyBytes, op.Revision)
		if err != nil {
			log.GetLogger().Error(err.Error())
			return err
		}
		defer func() {
			if err := ws.Cancel(watchID); err != nil {
				log.GetLogger().Error(err.Error())
			}
		}()
		responses := ws.Chan()
		for {
			select {
			case <-ctx.Done():
				return nil
			case resp, ok := <-responses:
				if !ok {
					err = errors.New("channel is closed")
					return err
				}

				err = dispatch(resp.Events, op.WatchCallback)
				if err != nil {
					return err
				}
			}
		}
	}
	err = fmt.Errorf("no key has been watched")
	return err
}

func (s *EtcdEmbed) ListCluster(ctx context.Context) (etcdadpt.Clusters, error) {
	clusters := etcdadpt.ParseClusters(s.Cfg.ClusterName, s.Cfg.ClusterAddresses, s.Cfg.ManagerAddress)
	return clusters, nil
}

func (s *EtcdEmbed) Status(ctx context.Context) (*etcdadpt.StatusResponse, error) {
	return &etcdadpt.StatusResponse{DBSize: s.Embed.Server.Backend().Size()}, nil
}

func (s *EtcdEmbed) readyNotify() {
	timeout := s.Cfg.DialTimeout
	select {
	case <-s.Embed.Server.ReadyNotify():
		close(s.ready)
		s.goroutine.Do(func(ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case err := <-s.Embed.Err():
				s.err <- err
			}
		})
	case <-time.After(timeout):
		err := fmt.Errorf("timed out(%s)", timeout)
		log.GetLogger().Error(fmt.Sprintf("read notify failed, error: %s", err))

		s.Embed.Server.Stop()

		s.err <- err
	}
}

func (s *EtcdEmbed) WithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.Cfg.RequestTimeOut)
}

func (s *EtcdEmbed) logRecover(r interface{}) {
	log.GetLogger().Error(fmt.Sprintf("embedded etcd recover: %v", r))
}

func dispatch(evts []mvccpb.Event, cb etcdadpt.WatchCallback) error {
	l := len(evts)
	kvs := make([]*mvccpb.KeyValue, l)
	sIdx, eIdx, rev := 0, 0, int64(0)
	action, prevEvtType := etcdadpt.ActionPut, mvccpb.PUT

	for _, evt := range evts {
		if prevEvtType != evt.Type {
			if eIdx > 0 {
				err := callback(action, rev, kvs[sIdx:eIdx], cb)
				if err != nil {
					return err
				}
				sIdx = eIdx
			}
			prevEvtType = evt.Type
		}

		if rev < evt.Kv.ModRevision {
			rev = evt.Kv.ModRevision
		}
		action = setKvsAndConvertAction(kvs, eIdx, evt)

		eIdx++
	}

	if eIdx > 0 {
		return callback(action, rev, kvs[sIdx:eIdx], cb)
	}
	return nil
}

func setKvsAndConvertAction(kvs []*mvccpb.KeyValue, pIdx int, evt mvccpb.Event) etcdadpt.Action {
	switch evt.Type {
	case mvccpb.DELETE:
		kv := evt.PrevKv
		if kv == nil {
			kv = evt.Kv
		}
		kvs[pIdx] = kv
		return etcdadpt.ActionDelete
	default:
		kvs[pIdx] = evt.Kv
		return etcdadpt.ActionPut
	}
}

func callback(action etcdadpt.Action, rev int64, kvs []*mvccpb.KeyValue, cb etcdadpt.WatchCallback) error {
	return cb("key information changed", &etcdadpt.Response{
		Action:    action,
		Kvs:       kvs,
		Count:     int64(len(kvs)),
		Revision:  rev,
		Succeeded: true,
	})
}

func NewEmbeddedEtcd(cfg etcdadpt.Config) etcdadpt.Client {
	inst := &EtcdEmbed{
		Cfg:   cfg,
		err:   make(chan error, 1),
		ready: make(chan struct{}),
	}
	log.GetLogger().Warn("enable embedded registry mode")

	hostName := "sc-0"
	if len(cfg.ClusterName) > 0 {
		hostName = cfg.ClusterName
	}
	mgrAddrs := "http://127.0.0.1:2380"
	if len(cfg.ManagerAddress) > 0 {
		mgrAddrs = cfg.ManagerAddress
	}
	inst.goroutine = gopool.New(gopool.Configure().WithRecoverFunc(inst.logRecover))

	if cfg.SslEnabled {
		log.GetLogger().Info("config no use for embedded etcd")
	}

	serverCfg := embed.NewConfig()
	serverCfg.EnableV2 = false
	serverCfg.EnablePprof = false
	serverCfg.QuotaBackendBytes = etcdserver.MaxQuotaBytes
	// TODO log
	// serverCfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(log.GetLogger())
	// TODO 不支持使用TLS通信
	// 存储目录，相对于工作目录
	serverCfg.Dir = DefaultDataDir
	// 集群支持
	serverCfg.Name = hostName
	serverCfg.InitialCluster = hostName + "=" + mgrAddrs
	// 1. 业务端口，默认2379端口关闭
	serverCfg.LCUrls = nil
	serverCfg.ACUrls = nil
	if len(cfg.ClusterAddresses) > 0 {
		urls, err := parseURL(cfg.ClusterAddresses)
		if err != nil {
			log.GetLogger().Error(fmt.Sprintf(`"ClusterAddresses" field configure error: %s`, err))
			inst.err <- err
			return inst
		}
		serverCfg.LCUrls = urls
		serverCfg.ACUrls = urls
	}
	// 2. 管理端口
	urls, err := parseURL(mgrAddrs)
	if err != nil {
		log.GetLogger().Error(fmt.Sprintf(`"ManagerAddress" field configure error: %s`, err))
		inst.err <- err
		return inst
	}
	serverCfg.LPUrls = urls
	serverCfg.APUrls = urls
	// 压缩配置项
	if cfg.CompactIndexDelta > 0 {
		serverCfg.AutoCompactionMode = v3compactor.ModeRevision
		serverCfg.AutoCompactionRetention = strconv.FormatInt(cfg.CompactIndexDelta, 10)
	} else if cfg.CompactInterval > 0 {
		// 自动压缩历史, 1 hour
		serverCfg.AutoCompactionMode = v3compactor.ModePeriodic
		serverCfg.AutoCompactionRetention = cfg.CompactInterval.String()
	}

	etcd, err := embed.StartEtcd(serverCfg)
	if err != nil {
		log.GetLogger().Error(fmt.Sprintf("error to start etcd server, error: %s", err))
		inst.err <- err
		return inst
	}
	inst.Embed = etcd

	inst.readyNotify()
	return inst
}

func parseURL(addrs string) ([]url.URL, error) {
	var urls []url.URL
	ips := strings.Split(addrs, ",")
	for _, ip := range ips {
		arr := strings.Split(ip, "=")
		addr, err := url.Parse(arr[len(arr)-1])
		if err != nil {
			return nil, err
		}
		urls = append(urls, *addr)
	}
	return urls, nil
}

func max(n1, n2 int64) int64 {
	if n1 > n2 {
		return n1
	}
	return n2
}
