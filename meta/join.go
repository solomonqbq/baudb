package meta


import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/tcp"
	"github.com/baudb/baudb/tcp/client"
	"github.com/baudb/baudb/util/os/fileutil"
	"github.com/baudb/baudb/vars"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

const localMetaFile = ".shard.%s.json"

type ShardMeta struct {
	ShardID string `json:"shard_id,omitempty"`
}

type ShardManager struct {
	meta ShardMeta
	sync.RWMutex
}

func (shardMgr *ShardManager) GetShardID() string {
	shardMgr.RLock()
	defer shardMgr.RUnlock()
	return shardMgr.meta.ShardID
}

func (shardMgr *ShardManager) JoinClusterByAddr(address string) (string, error) {
	cli := client.NewBackendClient("joinCli", address, 1, 0)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := cli.SyncRequest(ctx, &backendmsg.AdminCmdJoinCluster{})
	cancel()

	if err != nil {
		return "", err
	}

	generalResp, ok := resp.(*msg.GeneralResponse)
	if !ok {
		return "", tcp.BadMsgFormat
	}
	if generalResp.Status != msg.StatusCode_Succeed {
		return "", errors.New(generalResp.Message)
	}

	return shardMgr.JoinCluster(generalResp.Message)
}

func (shardMgr *ShardManager) JoinCluster(shardID string) (string, error) {
	if shardID == "" {
		if err := shardMgr.loadMeta(); err != nil {
			shardMgr.storeMeta(ShardMeta{strings.Replace(uuid.NewV1().String(), "-", "", -1)})
		}
	} else {
		shardMgr.storeMeta(ShardMeta{shardID})
	}

	return shardMgr.GetShardID(), nil
}

func (shardMgr *ShardManager) DisJoinCluster() {
	shardMgr.storeMeta(ShardMeta{})
}

func (shardMgr *ShardManager) loadMeta() error {
	usr, err := user.Current()
	if err != nil {
		return err
	}

	b, err := ioutil.ReadFile(filepath.Join(usr.HomeDir, fmt.Sprintf(localMetaFile, vars.Cfg.TcpPort)))
	if err != nil {
		return err
	}

	shardMgr.Lock()
	defer shardMgr.Unlock()

	return json.Unmarshal(b, &shardMgr.meta)
}

func (shardMgr *ShardManager) storeMeta(meta ShardMeta) error {
	usr, err := user.Current()
	if err != nil {
		return err
	}

	path := filepath.Join(usr.HomeDir, fmt.Sprintf(localMetaFile, vars.Cfg.TcpPort))
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")
	err = enc.Encode(&meta)
	if err != nil {
		f.Close()
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	err = fileutil.RenameFile(tmp, path)
	if err != nil {
		return err
	}

	shardMgr.Lock()
	shardMgr.meta = meta
	shardMgr.Unlock()

	return nil
}

