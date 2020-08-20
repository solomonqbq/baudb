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
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/baudb/baudb/datanode/storage"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/tcp/client"
	"github.com/baudb/baudb/util/os/fileutil"
	t "github.com/baudb/baudb/util/time"
	. "github.com/baudb/baudb/vars"
	"github.com/go-kit/kit/log/level"
)

const (
	metaFileName       = "meta.json"
	indexFileName      = "index"
	tombstonesFileName = "tombstones"
)

type Heartbeat struct {
	dbs                map[string]*storage.DB
	masterAddr         string
	shardID            string
	masterCli          *client.Client
	closed             uint32
	lastTSendHeartbeat int64
}

func (h *Heartbeat) sendHeartbeat(heartbeat *backendmsg.SyncHeartbeat) *backendmsg.SyncHeartbeatAck {
	if h.masterCli == nil {
		h.masterCli = client.NewBackendClient("rpl_s2m", h.masterAddr, 3, 0)
	}

	sleepTime := time.Second

	for h.isRunning() {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		reply, err := h.masterCli.SyncRequest(ctx, heartbeat)
		if err != nil {
			time.Sleep(sleepTime)
			sleepTime = t.Exponential(sleepTime, time.Second, time.Minute)
			continue
		} else {
			sleepTime = time.Second
		}

		ack, ok := reply.(*backendmsg.SyncHeartbeatAck)
		if !ok {
			level.Error(Logger).Log("error", "unexpected response")
			continue
		}

		atomic.StoreInt64(&h.lastTSendHeartbeat, t.FromTime(time.Now()))

		return ack
	}

	return nil
}

func (h *Heartbeat) keepAlive() {
	heartbeat := &backendmsg.SyncHeartbeat{
		MasterAddr: h.masterAddr,
		SlaveAddr:  fmt.Sprintf("%v:%v", LocalIP, Cfg.TcpPort),
		ShardID:    h.shardID,
	}

	for h.isRunning() {
		h.sendHeartbeat(heartbeat)
	}
}

func (h *Heartbeat) sync(db *storage.DB) {
	offset := getStartSyncOffset(db)
	if offset == nil {
		return
	}

	var (
		err         error
		dbDir       = db.Dir()
		fileSyncing *os.File
		heartbeat   = &backendmsg.SyncHeartbeat{
			MasterAddr:    h.masterAddr,
			SlaveAddr:     fmt.Sprintf("%v:%v", LocalIP, Cfg.TcpPort),
			ShardID:       h.shardID,
			BlkSyncOffset: offset,
		}
	)

	for h.isRunning() {
		ack := h.sendHeartbeat(heartbeat)
		if ack == nil {
			continue
		}

		if ack.Status != msg.StatusCode_Succeed {
			level.Error(Logger).Log("error", ack.Message)
			return
		}

		if ack.BlkSyncOffset != heartbeat.BlkSyncOffset {
			if ack.BlkSyncOffset == nil {
				if heartbeat.BlkSyncOffset.Ulid != "" {
					preBlockDir := filepath.Join(dbDir, heartbeat.BlkSyncOffset.Ulid)
					fileutil.RenameFile(preBlockDir+".tmp", preBlockDir)
					db.Reload()
				}

				if fileSyncing != nil {
					fileSyncing.Close()
					fileSyncing = nil
				}
			} else {
				blockTmpDir := filepath.Join(dbDir, ack.BlkSyncOffset.Ulid) + ".tmp"
				if ack.BlkSyncOffset.Ulid != heartbeat.BlkSyncOffset.Ulid {
					preBlockDir := filepath.Join(dbDir, heartbeat.BlkSyncOffset.Ulid)
					fileutil.RenameFile(preBlockDir+".tmp", preBlockDir)

					chunksDir := filepath.Join(blockTmpDir, "chunks")
					if err := os.MkdirAll(chunksDir, 0777); err != nil {
						level.Error(Logger).Log("msg", "can't create chunks dir", "error", err)
						continue
					}
				}
				if fileSyncing == nil {
					fileSyncing, err = os.Create(filepath.Join(blockTmpDir, ack.BlkSyncOffset.Path))
				} else if !strings.HasSuffix(fileSyncing.Name(), ack.BlkSyncOffset.Path) {
					fileSyncing.Close()
					fileSyncing, err = os.Create(filepath.Join(blockTmpDir, ack.BlkSyncOffset.Path))
				}

				if err != nil { //TODO
					level.Error(Logger).Log("error", err, "block", ack.BlkSyncOffset.Ulid, "path", ack.BlkSyncOffset.Path)
					continue
				}

				heartbeat.BlkSyncOffset = ack.BlkSyncOffset
			}
		}

		if ack.BlkSyncOffset == nil {
			return
		}

		for len(ack.Data) > 0 {
			written, err := fileSyncing.Write(ack.Data)
			heartbeat.BlkSyncOffset.Offset += int64(written)
			ack.Data = ack.Data[written:]

			if err != nil {
				level.Error(Logger).Log("error", err, "block", ack.BlkSyncOffset.Ulid, "path", ack.BlkSyncOffset.Path)
			}
		}
	}

	return
}

func (h *Heartbeat) start() {
	for _, db := range h.dbs {
		h.sync(db)
	}

	h.keepAlive()
}

func (h *Heartbeat) stop() {
	if atomic.CompareAndSwapUint32(&h.closed, 0, 1) {
		if h.masterCli != nil {
			h.masterCli.Close()
		}
	}
}

func (h *Heartbeat) isRunning() bool {
	return atomic.LoadUint32(&h.closed) == 0
}

func getStartSyncOffset(db *storage.DB) *backendmsg.BlockSyncOffset {
	blocks := db.Blocks()

	if len(blocks) == 0 {
		return &backendmsg.BlockSyncOffset{DB: db.Name()}
	}

	lastBlock := blocks[len(blocks)-1]

	files, err := ioutil.ReadDir(filepath.Join(lastBlock.Dir(), "chunks"))
	if err != nil {
		return nil
	}

	var (
		max       uint64
		lastChunk os.FileInfo
	)
	for _, fi := range files {
		if i, err := strconv.ParseUint(fi.Name(), 10, 64); err == nil && i > max {
			max = i
			lastChunk = fi
		}
	}

	if lastChunk == nil {
		err = os.RemoveAll(lastBlock.Dir()) //not clean, we clear it
		if err != nil {
			return nil
		}

		db.Reload()
		return getStartSyncOffset(db)
	}

	return &backendmsg.BlockSyncOffset{
		DB:     db.Name(),
		Ulid:   lastBlock.String(),
		MinT:   lastBlock.MinTime(),
		Path:   filepath.Join("chunks", lastChunk.Name()),
		Offset: lastChunk.Size(),
	}
}
