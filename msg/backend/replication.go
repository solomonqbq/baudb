//go:generate msgp -io=false -tests=false

package backend

import (
	"github.com/baudb/baudb/msg"
)

type HandshakeStatus byte

const (
	HandshakeStatus_FailedToSync HandshakeStatus = iota
	HandshakeStatus_NoLongerMySlave
	HandshakeStatus_NewSlave
	HandshakeStatus_AlreadyMySlave
)

func (z HandshakeStatus) String() string {
	switch z {
	case HandshakeStatus_FailedToSync:
		return "FailedToSync"
	case HandshakeStatus_NoLongerMySlave:
		return "NoLongerMySlave"
	case HandshakeStatus_NewSlave:
		return "NewSlave"
	case HandshakeStatus_AlreadyMySlave:
		return "AlreadyMySlave"
	}
	return "<Invalid>"
}

type SlaveOfCommand struct {
	MasterAddr string `msg:"masterAddr"`
}

type SyncHandshake struct {
	SlaveAddr    string `msg:"slaveAddr"`
	SlaveOfNoOne bool   `msg:"slaveOfNoOne"`
}

type SyncHandshakeAck struct {
	Status  HandshakeStatus `msg:"status"`
	ShardID string          `msg:"shardID"`
	Message string          `msg:"message"`
}

type BlockSyncOffset struct {
	DB     string `msg:"db"`
	Ulid   string `msg:"ulid"`
	MinT   int64  `msg:"minT"`
	Path   string `msg:"path"`
	Offset int64  `msg:"Offset"`
}

type SyncHeartbeat struct {
	MasterAddr    string           `msg:"masterAddr"`
	SlaveAddr     string           `msg:"slaveAddr"`
	ShardID       string           `msg:"shardID"`
	BlkSyncOffset *BlockSyncOffset `msg:"blkSyncOffset"`
}

type SyncHeartbeatAck struct {
	Status        msg.StatusCode   `msg:"status"`
	Message       string           `msg:"message"`
	BlkSyncOffset *BlockSyncOffset `msg:"blkSyncOffset"`
	Data          []byte           `msg:"data"`
}
