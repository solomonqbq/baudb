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

package replication

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baudb/baudb/datanode/storage"
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/tcp/client"
	t "github.com/baudb/baudb/util/time"
	. "github.com/baudb/baudb/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

type ReplicateManager struct {
	dbs                map[string]*storage.DB
	shardMgr           *meta.ShardManager
	sampleFeeds        sync.Map //map[string]*sampleFeed
	snapshot           *Snapshot
	heartbeat          *Heartbeat
	lastTRecvHeartbeat int64
	sync.RWMutex
}

func NewReplicateManager(dbs []*storage.DB, shardMgr *meta.ShardManager) *ReplicateManager {
	all := make(map[string]*storage.DB, len(dbs))
	for _, db := range dbs {
		all[db.Name()] = db
	}
	return &ReplicateManager{
		dbs:      all,
		shardMgr: shardMgr,
		snapshot: newSnapshot(all),
	}
}

func (mgr *ReplicateManager) HandleWriteReq(request []byte) {
	var toDelete []string

	mgr.sampleFeeds.Range(func(name, feed interface{}) bool {
		err := feed.(*sampleFeed).Write(request)
		if err != nil {
			if err == io.ErrShortWrite {
				toDelete = append(toDelete, name.(string))
			} else {
				level.Error(Logger).Log("msg", "failed to feed slave", "err", err)
			}
		}
		return true
	})

	if len(toDelete) > 0 {
		for _, name := range toDelete {
			feed, found := mgr.sampleFeeds.Load(name)
			if found {
				feed.(*sampleFeed).Close()
				mgr.sampleFeeds.Delete(name)
			}
			level.Warn(Logger).Log("msg", "slave was removed", "slaveAddr", name)
		}
	}
}

func (mgr *ReplicateManager) HandleHeartbeat(heartbeat *backendmsg.SyncHeartbeat) *backendmsg.SyncHeartbeatAck {
	//level.Info(Logger).Log("msg", "receive heartbeat from slave", "slaveAddr", heartbeat.SlaveAddr)
	atomic.StoreInt64(&mgr.lastTRecvHeartbeat, t.FromTime(time.Now()))

	var feed *sampleFeed

	feedI, found := mgr.sampleFeeds.Load(heartbeat.SlaveAddr)
	if !found {
		feed = NewSampleFeed(heartbeat.SlaveAddr, int(Cfg.Storage.Replication.HandleOffSize))
		mgr.sampleFeeds.Store(heartbeat.SlaveAddr, feed)
	} else {
		feed = feedI.(*sampleFeed)
		feed.FlushIfNeeded()
	}

	if heartbeat.BlkSyncOffset == nil {
		mgr.snapshot.Reset()
		return &backendmsg.SyncHeartbeatAck{
			Status: msg.StatusCode_Succeed,
		}
	}
	if _, found := mgr.dbs[heartbeat.BlkSyncOffset.DB]; !found {
		return &backendmsg.SyncHeartbeatAck{
			Status: msg.StatusCode_Succeed,
		}
	}

	mgr.snapshot.Init(feed.epoch)

	db := heartbeat.BlkSyncOffset.DB
	ulid := heartbeat.BlkSyncOffset.Ulid
	minT := heartbeat.BlkSyncOffset.MinT
	path := heartbeat.BlkSyncOffset.Path
	offset := heartbeat.BlkSyncOffset.Offset

	if ulid == "" {
		block := mgr.snapshot.NextBlock(db, heartbeat.BlkSyncOffset)
		if block == nil {
			return &backendmsg.SyncHeartbeatAck{
				Status: msg.StatusCode_Succeed,
			}
		}

		ulid = block.ULID.String()
		minT = block.MinTime
		path = metaFileName
		offset = 0
	}

	f, err := os.Open(mgr.snapshot.Path(db, ulid, path))
	if err != nil {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}
	defer f.Close()

	f.Seek(offset, 0)

	bytes := make([]byte, 256*PageSize)

	n, err := f.Read(bytes)
	if err == nil {
		return &backendmsg.SyncHeartbeatAck{
			Status: msg.StatusCode_Succeed,
			BlkSyncOffset: &backendmsg.BlockSyncOffset{
				DB:     db,
				Ulid:   ulid,
				MinT:   minT,
				Path:   path,
				Offset: offset,
			},
			Data: bytes[:n],
		}
	}

	if err != io.EOF {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}

	path, err = mgr.snapshot.NextPath(db, ulid, path)
	if err != nil {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}
	offset = 0

	if path == "" {
		block := mgr.snapshot.NextBlock(db, heartbeat.BlkSyncOffset)
		if block == nil {
			return &backendmsg.SyncHeartbeatAck{
				Status: msg.StatusCode_Succeed,
			}
		}

		ulid = block.ULID.String()
		minT = block.MinTime
		path = metaFileName
	}

	return &backendmsg.SyncHeartbeatAck{
		Status: msg.StatusCode_Succeed,
		BlkSyncOffset: &backendmsg.BlockSyncOffset{
			DB:     db,
			Ulid:   ulid,
			MinT:   minT,
			Path:   path,
			Offset: offset,
		},
	}
}

func (mgr *ReplicateManager) HandleSyncHandshake(handshake *backendmsg.SyncHandshake) *backendmsg.SyncHandshakeAck {
	if handshake.SlaveOfNoOne {
		feed, found := mgr.sampleFeeds.Load(handshake.SlaveAddr)
		if found {
			feed.(*sampleFeed).Close()
			mgr.sampleFeeds.Delete(handshake.SlaveAddr)
			level.Info(Logger).Log("msg", "slave was removed", "slaveAddr", handshake.SlaveAddr)
		}

		return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_NoLongerMySlave, ShardID: mgr.shardMgr.GetShardID()}
	}

	feed, found := mgr.sampleFeeds.Load(handshake.SlaveAddr)
	if !found { //to be new slave
		mgr.shardMgr.JoinCluster("")
		feed := NewSampleFeed(handshake.SlaveAddr, int(Cfg.Storage.Replication.HandleOffSize))
		mgr.sampleFeeds.Store(handshake.SlaveAddr, feed)

		//TODO generate snapshot

		return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_NewSlave, ShardID: mgr.shardMgr.GetShardID()}
	} else {
		feed.(*sampleFeed).FlushIfNeeded()
	}

	return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_AlreadyMySlave, Message: "already my slave", ShardID: mgr.shardMgr.GetShardID()}
}

func (mgr *ReplicateManager) HandleSlaveOfCmd(slaveOfCmd *backendmsg.SlaveOfCommand) *msg.GeneralResponse {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.heartbeat != nil {
		if slaveOfCmd.MasterAddr == "" { //slaveof no one
			mgr.heartbeat.stop()
			ack, err := mgr.syncHandshake(mgr.heartbeat.masterAddr, true)
			mgr.heartbeat = nil

			if err != nil {
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: err.Error()}
			}

			if ack.Status != backendmsg.HandshakeStatus_NoLongerMySlave {
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: fmt.Sprintf("status:%s, message:%s", ack.Status, ack.Message)}
			}
			mgr.shardMgr.DisJoinCluster()

			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed}
		} else if mgr.heartbeat.masterAddr == slaveOfCmd.MasterAddr {
			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: "already my slave"}
		} else {
			return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: "already an slave of some other master, clean dirty data first"}
		}
	} else {
		if slaveOfCmd.MasterAddr == "" { //slaveof no one
			mgr.shardMgr.DisJoinCluster()
			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed}
		} else {
			ack, err := mgr.syncHandshake(slaveOfCmd.MasterAddr, false)
			if err != nil {
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: err.Error()}
			}
			switch ack.Status {
			case backendmsg.HandshakeStatus_FailedToSync:
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: ack.Message}
			case backendmsg.HandshakeStatus_NoLongerMySlave:
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: fmt.Sprintf("unexpected slave of no one, ack msg: %s", ack.Message)}
			case backendmsg.HandshakeStatus_NewSlave:
				for _, db := range mgr.dbs {
					db.Head().Reset() //clear dirty data in memory
				}
			case backendmsg.HandshakeStatus_AlreadyMySlave:
				level.Info(Logger).Log("msg", "already an slave of the master", "masterAddr", slaveOfCmd.MasterAddr)
			}

			mgr.heartbeat = &Heartbeat{
				dbs:        mgr.dbs,
				masterAddr: slaveOfCmd.MasterAddr,
				shardID:    ack.ShardID,
			}
			go mgr.heartbeat.start()

			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: ack.Message}
		}
	}
}

func (mgr *ReplicateManager) syncHandshake(masterAddr string, slaveOfNoOne bool) (*backendmsg.SyncHandshakeAck, error) {
	masterCli := client.NewBackendClient("rpl_s2m", masterAddr, 1, 0)
	defer masterCli.Close()

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	reply, err := masterCli.SyncRequest(ctx, &backendmsg.SyncHandshake{
		SlaveAddr:    fmt.Sprintf("%v:%v", LocalIP, Cfg.TcpPort),
		SlaveOfNoOne: slaveOfNoOne,
	})
	if err != nil {
		return nil, err
	}

	ack, ok := reply.(*backendmsg.SyncHandshakeAck)
	if !ok {
		return nil, errors.New("unexpected response")
	}

	if shardID := mgr.shardMgr.GetShardID(); shardID == "" {
		mgr.shardMgr.JoinCluster(ack.ShardID)
		return ack, nil
	} else if shardID == ack.ShardID {
		return ack, nil
	}

	return nil, errors.New("unexpected shard id")
}

func (mgr *ReplicateManager) Master() (found bool, masterAddr string) {
	if mgr.heartbeat != nil {
		return true, mgr.heartbeat.masterAddr
	}

	return false, ""
}

func (mgr *ReplicateManager) Slaves() (slaveAddrs []string) {
	mgr.sampleFeeds.Range(func(key, value interface{}) bool {
		slaveAddrs = append(slaveAddrs, key.(string))
		return true
	})
	return
}

func (mgr *ReplicateManager) Close() error {
	if mgr.heartbeat != nil {
		mgr.heartbeat.stop()
	}
	mgr.sampleFeeds.Range(func(name, feed interface{}) bool {
		feed.(*sampleFeed).Close()
		mgr.sampleFeeds.Delete(name)
		return true
	})
	return nil
}

func (mgr *ReplicateManager) LastHeartbeatTime() (recv, send int64) {
	recv = atomic.LoadInt64(&mgr.lastTRecvHeartbeat)
	if mgr.heartbeat != nil {
		send = atomic.LoadInt64(&mgr.heartbeat.lastTSendHeartbeat)
	}
	return
}
