package meta

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"

	"github.com/baudb/baudb/util/os/fileutil"
	"github.com/baudb/baudb/vars"
)

const metaFile = ".baudb.%s.json"

type LocalMeta struct {
	ShardID string `json:"shard_id,omitempty"`
}

func LoadLocalMeta() (LocalMeta, error) {
	usr, err := user.Current()
	if err != nil {
		return LocalMeta{}, err
	}

	b, err := ioutil.ReadFile(filepath.Join(usr.HomeDir, fmt.Sprintf(metaFile, vars.Cfg.TcpPort)))
	if err != nil {
		return LocalMeta{}, err
	}

	var m LocalMeta
	err = json.Unmarshal(b, &m)
	return m, err
}

func StoreLocalMeta(localMeta LocalMeta) error {
	usr, err := user.Current()
	if err != nil {
		return err
	}

	path := filepath.Join(usr.HomeDir, fmt.Sprintf(metaFile, vars.Cfg.TcpPort))
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")
	err = enc.Encode(&localMeta)
	if err != nil {
		f.Close()
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	return fileutil.RenameFile(tmp, path)
}
