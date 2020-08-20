package replication

import (
	"fmt"
	"github.com/baudb/baudb/vars"
	"github.com/go-kit/kit/log/level"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/baudb/baudb/datanode/storage"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/util/os/fileutil"
)

type Snapshot struct {
	epoch int64

	dbs  map[string]*storage.DB
	dirs map[string]string

	inited bool
}

func newSnapshot(dbs map[string]*storage.DB) *Snapshot {
	return &Snapshot{
		dbs:    dbs,
		dirs:   make(map[string]string),
		inited: false,
	}
}

func (s *Snapshot) Init(epoch int64) {
	if !s.inited || s.epoch != epoch {
		s.epoch = epoch

		var snapDir string
		var err error

		for dbName, db := range s.dbs {
			snapDir = filepath.Join(db.Dir(), fmt.Sprintf("rpl_%d", epoch))
			if fileutil.Exist(snapDir) {
				fileutil.ClearPath(snapDir)
			}
			err = db.Snapshot(snapDir, true)
			if err == nil {
				s.dirs[dbName] = snapDir
			} else {
				level.Error(vars.Logger).Log("msg", "snapshotting failed", "db", dbName)
			}
		}

		s.inited = true
	}
}

func (s *Snapshot) Reset() {
	if len(s.dirs) > 0 {
		for _, snapDir := range s.dirs {
			os.RemoveAll(snapDir)
		}
	}
	s.inited = false
	s.epoch = 0
}

func (s *Snapshot) Dir(dbName string) string {
	return s.dirs[dbName]
}

func (s *Snapshot) Path(dbName string, ulid string, file string) string {
	return filepath.Join(s.Dir(dbName), ulid, file)
}

func (s *Snapshot) NextBlock(dbName string, offset *backendmsg.BlockSyncOffset) *storage.BlockMeta {
	db, found := s.dbs[dbName]
	if !found {
		return nil
	}

	blockMetas, err := db.SnapshotMeta(s.Dir(dbName))
	if err != nil {
		return nil
	}

	for _, block := range blockMetas {
		if block.MinTime >= s.epoch {
			break
		}

		if block.MinTime >= offset.MinT && block.ULID.String() != offset.Ulid {
			return block
		}
	}

	return nil
}

func (s *Snapshot) NextPath(dbName string, ulid string, currentPath string) (string, error) {
	path := ""
	blockDir := filepath.Join(s.Dir(dbName), ulid)

	if currentPath == metaFileName {
		path = indexFileName
	} else if currentPath == indexFileName {
		path = tombstonesFileName
	} else if currentPath == tombstonesFileName {
		path = filepath.Join("chunks", fmt.Sprintf("%0.6d", 1))
	} else {
		i, err := strconv.ParseUint(strings.TrimPrefix(currentPath, "chunks"+string(os.PathSeparator)), 10, 64)
		if err != nil {
			return "", err
		}

		path = filepath.Join("chunks", fmt.Sprintf("%0.6d", i+1))
	}

	if _, err := os.Stat(filepath.Join(blockDir, path)); os.IsNotExist(err) {
		return "", nil
	}

	return path, nil
}
