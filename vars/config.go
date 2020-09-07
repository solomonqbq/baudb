package vars

import (
	"time"

	"github.com/baudb/baudb/util/toml"
)

type EtcdConfig struct {
	Endpoints     []string      `toml:"endpoints"`
	DialTimeout   toml.Duration `toml:"dial_timeout"`
	RWTimeout     toml.Duration `toml:"rw_timeout"`
	RetryNum      int           `toml:"retry_num"`
	RetryInterval toml.Duration `toml:"retry_interval"`
}

type RouteConfig struct {
	ShardGroupTTL          toml.Duration `toml:"shard_group_ttl"`
	ShardGroupTickInterval toml.Duration `toml:"shard_group_tick_interval"`
	HostMinDGBiskLeft      uint64        `toml:"host_min_gb_disk_left"`
}

type AppenderConfig struct {
	AsyncTransfer bool          `toml:"async_transfer"`
	Timeout       toml.Duration `toml:"timeout"`
}

type QueryEngineConfig struct {
	Concurrency int           `toml:"concurrency"`
	Timeout     toml.Duration `toml:"timeout"`
}

type RuleConfig struct {
	EvalInterval toml.Duration `toml:"eval_interval"`
	RuleFileDir  string        `toml:"rules_dir"`
}

type GatewayConfig struct {
	SyncConnsPerBackend  int                `toml:"sync_conns_per_backend"`
	AsyncConnsPerBackend int                `toml:"async_conns_per_backend"`
	QueryStrategy        string             `toml:"query_strategy"`
	Route                RouteConfig        `toml:"route"`
	Appender             *AppenderConfig    `toml:"appender,omitempty"`
	QueryEngine          *QueryEngineConfig `toml:"query_engine,omitempty"`
	Rule                 *RuleConfig        `toml:"rule,omitempty"`
}

type TSDBConfig struct {
	Paths             []string      `toml:"paths"`
	RetentionDuration toml.Duration `toml:"retention_duration"` // Duration of persisted data to keep.
	BlockRanges       []int64       `toml:"block_ranges"`       // The sizes of the Blocks.
	EnableWal         bool          `toml:"enable_wal,omitempty"`
	NoLockfile        bool          `toml:"no_lockfile,omitempty"` // NoLockfile disables creation and consideration of a lock file.
}

type StatReportConfig struct {
	HeartbeartInterval toml.Duration `toml:"heartbeart_interval"`
	SessionExpireTTL   toml.Duration `toml:"session_expire_ttl"`
}

type ReplicationConfig struct {
	SampleFeedConnsNum int           `toml:"sample_feed_conns_num"`
	HandleOffSize      toml.Size     `toml:"handleoff_size"`
	HeartbeatInterval  toml.Duration `toml:"heartbeart_interval"`
}

type StorageConfig struct {
	TSDB        TSDBConfig         `toml:"tsdb"`
	StatReport  StatReportConfig   `toml:"stat_report"`
	Replication *ReplicationConfig `toml:"replication"`
}

type JaegerConfig struct {
	SamplerType       string `toml:"sampler_type"`
	SampleNumPerSec   int    `toml:"sample_num_per_sec"`
	AgentHostPort     string `toml:"agent_host_port"`
	CollectorEndpoint string `toml:"collector_endpoint"`
}

type CompactConfig struct {
	LowWaterMark  toml.Size `toml:"low_watermark"`
	HighWaterMark toml.Size `toml:"high_watermark"`
}

type LimitConfig struct {
	RLimit     uint64        `toml:"rlimit,omitempty"`
	InboundKBS toml.Size     `toml:"inbound_kilo_bytes_per_sec,omitempty"`
	Compact    CompactConfig `toml:"compact"`
}

type Config struct {
	TcpPort       string         `toml:"tcp_port"`
	HttpPort      string         `toml:"http_port"`
	MaxConn       int            `toml:"max_conn"`
	NameSpace     string         `toml:"namespace,omitempty"`
	LookbackDelta toml.Duration  `toml:"lookback_delta"`
	Etcd          EtcdConfig     `toml:"etcd"`
	Limit         LimitConfig    `toml:"limit"`
	Gateway       *GatewayConfig `toml:"gateway,omitempty"`
	Storage       *StorageConfig `toml:"storage,omitempty"`
	Jaeger        *JaegerConfig  `toml:"jaeger,omitempty"`
}

var Cfg = Config{
	TcpPort:       "8121",
	HttpPort:      "8080",
	MaxConn:       10000,
	NameSpace:     "baudb",
	LookbackDelta: toml.Duration(5 * time.Second),

	Etcd: EtcdConfig{
		Endpoints:     []string{"localhost:2379"},
		DialTimeout:   toml.Duration(5 * time.Second),
		RWTimeout:     toml.Duration(15 * time.Second),
		RetryNum:      2,
		RetryInterval: toml.Duration(2 * time.Second),
	},
	Limit: LimitConfig{
		Compact: CompactConfig{
			LowWaterMark:  toml.Size(4 * G),
			HighWaterMark: toml.Size(8 * G),
		},
	},
}

func LoadConfig(tomlFile string) error {
	err := toml.LoadFromToml(tomlFile, &Cfg)
	if err != nil {
		return err
	}

	return nil
}
