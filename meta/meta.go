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
	"encoding/binary"
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/tcp"
	"github.com/baudb/baudb/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type Shard struct {
	ID          string
	Masters     []*Node
	Slaves      []*Node
	failovering uint32
}

//meta's responsibility is to provide our necessary data
type meta struct {
	shards           unsafe.Pointer //point to a map[string]*Shard
	route            RouteInfo
	refreshingShards uint32
	refreshingRoute  uint32
	watching         uint32
}

func (m *meta) getShardGroup(tickNO uint64) ([]string, error) {
	shardGroup, found := m.route.Get(tickNO)
	if found {
		return shardGroup, nil
	}
	shardGroup, err := m.getShardGroupFromEtcd(tickNO)
	if err != nil {
		return nil, err
	}
	m.route.Put(tickNO, shardGroup)
	return shardGroup, nil
}

func (m *meta) getShardGroupFromEtcd(tickNO uint64) ([]string, error) {
	level.Info(vars.Logger).Log("msg", "get shards from etcd", "tickNO", tickNO)

	var shardGroup []string

	key := routeInfoPrefix() + strconv.FormatUint(tickNO, 10)
	err := etcdGet(key, &shardGroup)
	if err == nil {
		return shardGroup, nil
	}

	if err == ErrKeyNotFound {
		return nil, nil
	}
	return nil, err
}

func (m *meta) createShardGroup(tickNO uint64) ([]string, error) {
	var shardGroup []string

	shards := m.AllShards()
	for _, shard := range shards {
		if shard.ID == "" {
			continue
		}

		found := false
		for _, shardID := range shardGroup {
			if shardID == shard.ID {
				found = true
				break
			}
		}
		if !found {
			shardGroup = append(shardGroup, shard.ID)
		}
	}

	if len(shardGroup) == 0 {
		return nil, errors.Errorf("not enough shards to init %v", tickNO)
	}
	sort.Strings(shardGroup)

	key := routeInfoPrefix() + strconv.FormatUint(tickNO, 10)
	err := etcdPut(key, shardGroup, clientv3.NoLease)
	if err != nil {
		return nil, err
	}

	evicted := m.route.Put(tickNO, shardGroup)
	for _, tickNO := range evicted {
		etcdDel(routeInfoPrefix() + strconv.FormatUint(tickNO, 10))
	}
	return shardGroup, nil
}

func (m *meta) GetShard(shardID string) (shard *Shard, found bool) {
	shards := (*map[string]*Shard)(atomic.LoadPointer(&m.shards))
	if shards != nil {
		shard, found = (*shards)[shardID]
	}
	return
}

func (m *meta) AllShards() map[string]*Shard {
	shards := (*map[string]*Shard)(atomic.LoadPointer(&m.shards))
	return *shards
}

func (m *meta) RefreshTopology() error {
	if !atomic.CompareAndSwapUint32(&m.refreshingShards, 0, 1) {
		return nil
	}
	defer atomic.StoreUint32(&m.refreshingShards, 0)

	shards := make(map[string]*Shard)

	nodes, err := AllNodes(false)
	if err != nil {
		return err
	}

	for i, node := range nodes {
		shard, found := shards[node.ShardID]
		if !found {
			shard = &Shard{ID: node.ShardID}
			shards[node.ShardID] = shard
		}

		if node.MasterIP == "" && node.MasterPort == "" {
			shard.Masters = append(shard.Masters, nodes[i])
		} else {
			shard.Slaves = append(shard.Slaves, nodes[i])
		}
	}

	for _, shard := range shards {
		sort.Sort(NodesSortable(shard.Masters))
		sort.Sort(NodesSortable(shard.Slaves))
	}

	atomic.StorePointer(&m.shards, (unsafe.Pointer)(&shards))
	return nil
}

func (m *meta) RefreshRoute() error {
	if !atomic.CompareAndSwapUint32(&m.refreshingRoute, 0, 1) {
		return nil
	}
	defer atomic.StoreUint32(&m.refreshingRoute, 0)

	resp, err := etcdGetWithPrefix(routeInfoPrefix())
	if err == ErrKeyNotFound {
		return nil
	}

	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		t := strings.TrimPrefix(string(kv.Key), routeInfoPrefix())
		tickNO, err := strconv.ParseUint(t, 10, 0)
		if err != nil {
			continue
		}

		var shardGroup []string
		err = json.Unmarshal(kv.Value, &shardGroup)
		if err != nil {
			continue
		}

		evicted := m.route.Put(tickNO, shardGroup)
		for _, tickNO := range evicted {
			etcdDel(routeInfoPrefix() + strconv.FormatUint(tickNO, 10))
		}
	}
	return nil
}

func (m *meta) watch() {
	if !atomic.CompareAndSwapUint32(&m.watching, 0, 1) {
		return
	}
	go func() {
		defer atomic.StoreUint32(&m.watching, 0)

		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   vars.Cfg.Etcd.Endpoints,
			DialTimeout: time.Duration(vars.Cfg.Etcd.DialTimeout),
		})
		if err != nil {
			level.Error(vars.Logger).Log("msg", "failed to connect to etcd", "err", err)
			os.Exit(1)
		}
		defer cli.Close()

		rch := cli.Watch(context.Background(), routeInfoPrefix(), clientv3.WithPrefix())
		nch := cli.Watch(context.Background(), nodePrefix(), clientv3.WithPrefix(), clientv3.WithPrevKV())

		var wresp clientv3.WatchResponse

		level.Info(vars.Logger).Log("msg", "i am watching etcd events now")
		for {
			select {
			case wresp = <-rch:
				for _, ev := range wresp.Events {
					level.Warn(vars.Logger).Log(
						"msg", "get etcd event",
						"type", ev.Type,
						"key", ev.Kv.Key,
						"value", ev.Kv.Value,
					)

					t := strings.TrimPrefix(string(ev.Kv.Key), routeInfoPrefix())
					tickNO, err := strconv.ParseUint(t, 10, 0)
					if err != nil {
						continue
					}

					if ev.Type == mvccpb.DELETE {
						m.route.Delete(tickNO)
					} else {
						var shardGroup []string
						if err = json.Unmarshal(ev.Kv.Value, &shardGroup); err == nil {
							evicted := m.route.Put(tickNO, shardGroup)
							for _, tickNO := range evicted {
								etcdDel(routeInfoPrefix() + strconv.FormatUint(tickNO, 10))
							}
						}
					}
				}
			case wresp = <-nch:
				for _, ev := range wresp.Events {
					if ev.Type == mvccpb.DELETE && ev.PrevKv != nil {
						level.Warn(vars.Logger).Log(
							"msg", "get etcd event",
							"type", ev.Type,
							"key", ev.Kv.Key,
							"value", ev.Kv.Value,
							"preKey", ev.PrevKv.Key,
							"preValue", ev.PrevKv.Value,
						)

						var node Node
						if err = json.Unmarshal(ev.PrevKv.Value, &node); err == nil {
							FailoverIfNeeded(node.ShardID)
						}
					}
				}
				m.RefreshTopology()
			}
		}
	}()
}

var (
	globalMeta meta
	initOnce   sync.Once
)

func Init() (err error) {
	initOnce.Do(func() {
		err = globalMeta.RefreshTopology()
		if err != nil {
			return
		}

		err = globalMeta.RefreshRoute()
		if err != nil {
			return
		}

		globalMeta.watch()

		level.Info(vars.Logger).Log("msg", "watching nodes")
	})
	return
}

func AllShards() map[string]*Shard {
	return globalMeta.AllShards()
}

func GetShard(shardID string) (*Shard, bool) {
	return globalMeta.GetShard(shardID)
}

func GetMasters(shardID string) []*Node {
	shard, found := globalMeta.GetShard(shardID)

	if !found || shard == nil {
		return nil
	}

	return shard.Masters
}

func GetSlaves(shardID string) []*Node {
	shard, found := globalMeta.GetShard(shardID)

	if !found || shard == nil {
		return nil
	}

	return shard.Slaves
}

func RefreshTopology() error {
	return globalMeta.RefreshTopology()
}

func FailoverIfNeeded(shardID string) {
	shard, found := globalMeta.GetShard(shardID)
	if !found || len(shard.Masters) > 0 {
		return
	}

	globalMeta.RefreshTopology()

	shard, found = globalMeta.GetShard(shardID)
	if !found || len(shard.Masters) > 0 {
		return
	}

	var (
		slaveOfReq      = tcp.Message{Message: &backendmsg.SlaveOfCommand{}}
		slaveOfReqBytes = make([]byte, 1+binary.MaxVarintLen64+slaveOfReq.SizeOfRaw())
		msgCodec        tcp.MsgCodec
	)

	n, err := msgCodec.Encode(slaveOfReq, slaveOfReqBytes) //slaveof no one
	if err != nil {
		level.Error(vars.Logger).Log("err", err)
		return
	}

	if !atomic.CompareAndSwapUint32(&shard.failovering, 0, 1) {
		return
	}
	defer atomic.StoreUint32(&shard.failovering, 0)

	failoverErr := mutexRun("failover", func(session *concurrency.Session) error {
		masters := GetMasters(shardID)
		if len(masters) > 0{ //already failover by other gateway
			return nil
		}

		slaves := GetSlaves(shardID)
		if len(slaves) == 0 {
			return errors.New("no available slave to failover")
		}

		chosen := slaves[0]

		slaveConn, err := tcp.Connect(chosen.Addr())
		if err != nil {
			return errors.Wrap(err, "failed to connect to slave")
		}
		defer slaveConn.Close()

		err = slaveConn.WriteMsg(slaveOfReqBytes[:n])
		if err != nil {
			return errors.Wrap(err, "failed to send slave of no one")
		}

		level.Warn(vars.Logger).Log("msg", "failover triggered", "shard", shardID, "chosen", chosen.Addr())

		c := make(chan struct{})
		go func() {
			defer close(c)

			slaveOfRespBytes, er := slaveConn.ReadMsg()
			if er != nil {
				return
			}

			reply, er := msgCodec.Decode(slaveOfRespBytes)
			if raw := reply.GetRaw(); er == nil && raw != nil {
				reply, ok := raw.(*msg.GeneralResponse)
				if !ok {
					return
				}

				if reply.Status != msg.StatusCode_Succeed {
					err = errors.New(reply.Message)
				} else {
					level.Warn(vars.Logger).Log("msg", "failover succeed", "shard", shardID, "chosen", chosen.Addr())
				}
			}
		}()

		select {
		case <-c:
		case <-time.After(15 * time.Second):
		}

		globalMeta.RefreshTopology()

		return err
	})

	if failoverErr != nil {
		level.Error(vars.Logger).Log("msg", "error occurred when failover", "shard", shardID, "err", failoverErr)
	}
}
