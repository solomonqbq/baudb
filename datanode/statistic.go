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

package datanode

import (
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/util"
	tm "github.com/baudb/baudb/util/time"
	"strconv"
	"sync/atomic"
	"time"
)

const lnBreak = byte('\n')

type OPStat struct {
	SucceedSel       uint64
	FailedSel        uint64
	SucceedLVals     uint64
	FailedLVals      uint64
	ReceivedAdd      uint64
	SucceedAdd       uint64
	FailedAdd        uint64
	ValueTooLong     uint64
	OutOfBounds      uint64
	FailedCommit     uint64
	LastValueTooLong int64
	LastOutOfBounds  int64
}

func (stat *OPStat) Reset() {
	atomic.StoreUint64(&stat.SucceedSel, 0)
	atomic.StoreUint64(&stat.FailedSel, 0)
	atomic.StoreUint64(&stat.SucceedLVals, 0)
	atomic.StoreUint64(&stat.FailedLVals, 0)
	atomic.StoreUint64(&stat.ReceivedAdd, 0)
	atomic.StoreUint64(&stat.SucceedAdd, 0)
	atomic.StoreUint64(&stat.FailedAdd, 0)
	atomic.StoreUint64(&stat.ValueTooLong, 0)
	atomic.StoreUint64(&stat.OutOfBounds, 0)
	atomic.StoreUint64(&stat.FailedCommit, 0)
	atomic.StoreInt64(&stat.LastValueTooLong, 0)
	atomic.StoreInt64(&stat.LastOutOfBounds, 0)
}

type DBStat struct {
	SeriesNum            uint64
	BlockNum             int
	HeadMinTime          int64
	HeadMaxTime          int64
	HeadMinValidTime     int64
	AppenderMinValidTime int64
	DiskUsage            uint64
}

type Stat struct {
	meta.Node
	OpStat     OPStat
	dbStats    []*DBStat
	LastRecvHb int64
	LastSendHb int64
}

func (stat Stat) String() string {
	var buf []byte

	buf = append(append(append(buf, "Shard: "...), stat.Node.ShardID...), lnBreak)
	buf = append(append(append(buf, "IP: "...), stat.Node.IP...), lnBreak)
	buf = append(append(append(buf, "Port: "...), stat.Node.Port...), lnBreak)
	buf = append(append(append(append(buf, "DiskFree: "...), strconv.FormatUint(stat.Node.DiskFree, 10)...), "GB"...), lnBreak)
	buf = append(append(append(buf, "IDC: "...), stat.Node.IDC...), lnBreak)

	if stat.Node.MasterIP != "" && stat.Node.MasterPort != "" {
		buf = append(append(append(buf, "MasterIP: "...), stat.Node.MasterIP...), lnBreak)
		buf = append(append(append(buf, "MasterPort: "...), stat.Node.MasterPort...), lnBreak)
	}

	buf = append(append(buf, lnBreak), lnBreak)

	buf = append(append(append(buf, "SucceedSel: "...), strconv.FormatUint(stat.OpStat.SucceedSel, 10)...), lnBreak)
	buf = append(append(append(buf, "FailedSel: "...), strconv.FormatUint(stat.OpStat.FailedSel, 10)...), lnBreak)
	buf = append(append(append(buf, "SucceedLVals: "...), strconv.FormatUint(stat.OpStat.SucceedLVals, 10)...), lnBreak)
	buf = append(append(append(buf, "FailedLVals: "...), strconv.FormatUint(stat.OpStat.FailedLVals, 10)...), lnBreak)
	buf = append(append(append(buf, "ReceivedAdd: "...), strconv.FormatUint(stat.OpStat.ReceivedAdd, 10)...), lnBreak)
	buf = append(append(append(buf, "SucceedAdd: "...), strconv.FormatUint(stat.OpStat.SucceedAdd, 10)...), lnBreak)
	buf = append(append(append(buf, "FailedAdd: "...), strconv.FormatUint(stat.OpStat.FailedAdd, 10)...), lnBreak)
	buf = append(append(append(buf, "ValueTooLong: "...), strconv.FormatUint(stat.OpStat.ValueTooLong, 10)...), lnBreak)
	buf = append(append(append(buf, "OutOfBounds: "...), strconv.FormatUint(stat.OpStat.OutOfBounds, 10)...), lnBreak)
	buf = append(append(append(buf, "FailedCommit: "...), strconv.FormatUint(stat.OpStat.FailedCommit, 10)...), lnBreak)
	buf = append(append(append(buf, "LastValueTooLong: "...), formatTimestamp(stat.OpStat.LastValueTooLong)...), lnBreak)
	buf = append(append(append(buf, "LastOutOfBounds: "...), formatTimestamp(stat.OpStat.LastOutOfBounds)...), lnBreak)

	if stat.LastRecvHb > 0 {
		buf = append(append(append(buf, "LastRecvHb: "...), formatTimestamp(stat.LastRecvHb)...), lnBreak)
	}
	if stat.LastSendHb > 0 {
		buf = append(append(buf, "LastSendHb: "...), formatTimestamp(stat.LastSendHb)...)
	}

	for _, dbStat := range stat.dbStats {
		buf = append(append(buf, lnBreak), lnBreak)
		buf = append(append(append(buf, "SeriesNum: "...), strconv.FormatUint(dbStat.SeriesNum, 10)...), lnBreak)
		buf = append(append(append(buf, "BlockNum: "...), strconv.Itoa(dbStat.BlockNum)...), lnBreak)
		buf = append(append(append(buf, "HeadMinTime: "...), formatTimestamp(dbStat.HeadMinTime)...), lnBreak)
		buf = append(append(append(buf, "HeadMaxTime: "...), formatTimestamp(dbStat.HeadMaxTime)...), lnBreak)
		buf = append(append(append(buf, "HeadMinValidTime: "...), formatTimestamp(dbStat.HeadMinValidTime)...), lnBreak)
		buf = append(append(append(buf, "AppenderMinValidTime: "...), formatTimestamp(dbStat.AppenderMinValidTime)...), lnBreak)
		buf = append(append(append(append(buf, "DiskUsage: "...), strconv.FormatUint(dbStat.DiskUsage, 10)...), "GB"...), lnBreak)
	}

	return util.YoloString(buf)
}

func formatTimestamp(t int64) string {
	if t <= 0 {
		return strconv.FormatInt(t, 10)
	}
	return tm.Time(t).Format(time.RFC3339)
}
