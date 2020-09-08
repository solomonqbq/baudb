/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/baudb/baudb/util"
	"github.com/baudb/baudb/util/redo"
	"github.com/baudb/baudb/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
)

type Node struct {
	ShardID    string
	IP         string
	Port       string
	DiskFree   uint64
	IDC        string
	MasterIP   string
	MasterPort string
	addr       *string
}

func (node *Node) Addr() string {
	if node.addr != nil {
		return *node.addr
	}

	var b strings.Builder
	b.WriteString(node.IP)
	b.WriteByte(':')
	b.WriteString(node.Port)
	addr := b.String()

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&node.addr)), unsafe.Pointer(&addr))

	return *node.addr
}

func (node *Node) MayOnline() bool {
	addr := node.Addr()
	if util.Ping(addr) {
		return true
	}

	nodeExist, err := exist(nodePrefix() + addr)
	if err != nil {
		return true
	}

	return nodeExist
}

type NodesSortable []*Node

func (nodes NodesSortable) Len() int {
	return len(nodes)
}

func (nodes NodesSortable) Swap(i, j int) {
	nodes[i], nodes[j] = nodes[j], nodes[i]
}

func (nodes NodesSortable) Less(i, j int) bool {
	return strings.Compare(nodes[i].IP, nodes[j].IP) < 0
}

func AllNodes(withSort bool) ([]*Node, error) {
	resp, err := etcdGetWithPrefix(nodePrefix())
	if err == ErrKeyNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	nodes := make(NodesSortable, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		node := new(Node)
		err = json.Unmarshal(kv.Value, node)
		if err != nil {
			return nil, err
		}

		if node.IP == "" || node.Port == "" {
			return nil, errors.Errorf("bad format %s", kv.Value)
		}
		nodes[i] = node
	}

	if withSort {
		sort.Sort(nodes)
	}

	return nodes, nil
}

type Heartbeat struct {
	cmtx         sync.RWMutex
	client       *clientv3.Client   //maybe reconnect
	leaseTTL     int64              //maybe reGrant
	leaseID      clientv3.LeaseID   //maybe rekeepalive
	cancel       context.CancelFunc //maybe used to maybe
	interval     time.Duration
	f            func() (Node, error)
	lastNodeInfo Node
	registerC    chan struct{}
	exitCh       chan struct{}
	wg           sync.WaitGroup
	started      uint32
}

func NewHeartbeat(leaseTTL time.Duration, reportInterval time.Duration, f func() (Node, error)) *Heartbeat {
	return &Heartbeat{
		leaseTTL:  int64(leaseTTL.Seconds()),
		interval:  reportInterval,
		f:         f,
		registerC: make(chan struct{}),
		exitCh:    make(chan struct{}),
	}
}

func (h *Heartbeat) Start() error {
	if !atomic.CompareAndSwapUint32(&h.started, 0, 1) {
		return nil
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   vars.Cfg.Etcd.Endpoints,
		DialTimeout: time.Duration(vars.Cfg.Etcd.DialTimeout),
	})
	if err != nil {
		return errors.Wrap(err, "can't init heartbeat etcd client")
	}
	h.client = cli

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.Etcd.RWTimeout))
	defer cancel()

	leaseResp, err := h.client.Grant(ctx, h.leaseTTL)
	if err != nil {
		return errors.Wrap(err, "can't init heartbeat etcd lease")
	}
	h.leaseID = leaseResp.ID

	h.wg.Add(1)
	go func() {
		h.keepLease()
		h.wg.Done()
	}()

	node, err := h.f()
	if err != nil {
		return errors.Wrap(err, "can't get node info")
	}

	err = h.reportInfo(node)
	if err != nil {
		return errors.Wrap(err, "can't register node")
	}

	h.wg.Add(1)
	go func() {
		h.cronReportInfo()
		h.wg.Done()
	}()

	return nil
}

func (h *Heartbeat) Stop() {
	close(h.exitCh)
	h.wg.Wait()

	if h.client != nil {
		redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
			ctx, cancel := context.WithTimeout(h.client.Ctx(), time.Duration(vars.Cfg.Etcd.RWTimeout))
			_, err := h.client.Delete(ctx, nodePrefix()+h.lastNodeInfo.Addr())
			cancel()
			if err != nil {
				return true, err
			}
			return false, nil
		})

		h.client.Revoke(context.Background(), h.leaseID)
		h.client.Close()
	}
}

func (h *Heartbeat) keepLease() {
	reGrant := false

	for {
		select {
		case <-h.exitCh:
			level.Warn(vars.Logger).Log("msg", "keep lease for heartbeat exit!!!")
			return
		default:
		}

		if reGrant {
			level.Warn(vars.Logger).Log("msg", "regrant for heartbeat")
			leaseResp, err := h.client.Grant(context.Background(), h.leaseTTL)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}

			h.leaseID = leaseResp.ID
			reGrant = false
		}

		ctx, cancel := context.WithCancel(context.Background())
		keepAlive, err := h.client.KeepAlive(ctx, h.leaseID)
		if err != nil || keepAlive == nil {
			cancel()
			reGrant = true

			h.lastNodeInfo = Node{}
			continue
		}

		h.cancel = cancel

		go func() {
			h.registerC <- struct{}{}
		}()

	keepAliveLoop:
		for {
			select {
			case _, ok := <-keepAlive:
				if !ok {
					break keepAliveLoop
				}
				// just eat messages
			case <-h.exitCh:
				level.Warn(vars.Logger).Log("msg", "keep lease for heartbeat exit!!!")
				return
			}
		}

		h.lastNodeInfo = Node{}
		reGrant = true
	}
}

func (h *Heartbeat) reportInfo(node Node) error {
	if h.lastNodeInfo.ShardID == node.ShardID &&
		h.lastNodeInfo.IP == node.IP &&
		h.lastNodeInfo.Port == node.Port &&
		h.lastNodeInfo.DiskFree == node.DiskFree &&
		h.lastNodeInfo.IDC == node.IDC {
		return nil
	}

	b, err := json.Marshal(&node)
	if err != nil {
		return err
	}

	h.cmtx.RLock()
	if h.client != nil && h.leaseID != clientv3.NoLease {
		err = redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
			ctx, cancel := context.WithTimeout(h.client.Ctx(), time.Duration(vars.Cfg.Etcd.RWTimeout))
			_, er := h.client.Put(ctx, nodePrefix()+node.Addr(), string(b), clientv3.WithLease(h.leaseID))
			cancel()
			if er != nil {
				return true, er
			}
			return false, nil
		})
	}
	h.cmtx.RUnlock()

	if err == nil {
		h.lastNodeInfo = node
	} else if h.cancel != nil {
		h.cancel() //let reconnect and keep lease again
	}

	return err
}

func (h *Heartbeat) cronReportInfo() {
	var (
		node Node
		err  error
	)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if node, err = h.f(); err == nil {
				h.reportInfo(node)
			}
		case <-h.registerC:
			if node, err = h.f(); err == nil {
				h.reportInfo(node)
			}
		case <-h.exitCh:
			level.Warn(vars.Logger).Log("msg", "report host info for heartbeat exit!!!")
			return
		}
	}
}
