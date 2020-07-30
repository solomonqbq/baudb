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

package storage

import (
	"bytes"
	"context"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baudb/baudb/backend/storage/filter"
	"github.com/baudb/baudb/backend/storage/labels"
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/tcp"
	"github.com/baudb/baudb/tcp/client"
	"github.com/baudb/baudb/util"
	"github.com/baudb/baudb/util/syn"
	"github.com/baudb/baudb/util/worker"
	"github.com/baudb/baudb/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/shirou/gopsutil/disk"
	"go.uber.org/multierr"
)

const selTimeout = 2 * time.Minute

var iterExecutor = worker.NewWorkerPool("iterExecutor", runtime.NumCPU())

func selectLabelsOnly(ctx context.Context, q Querier, matchers []labels.Matcher) ([]*msg.Series, error) {
	if parentSpan, ok := ctx.Value("span").(opentracing.Span); ok {
		selectLabelsOnly := opentracing.StartSpan("selectLabelsOnly", opentracing.ChildOf(parentSpan.Context()))
		defer selectLabelsOnly.Finish()
	}

	set, err := q.Select(matchers...)
	if err != nil {
		return nil, err
	}

	var series []*msg.Series

	ctx, cancel := context.WithTimeout(ctx, selTimeout)
	defer cancel()

	for set.Next() {
		curSeries := set.At()
		series = append(series, &msg.Series{
			Labels: LabelsToProto(curSeries.Labels()),
			Points: nil,
		})
	}

	return series, nil
}

func selectSeries(ctx context.Context, q Querier, matchers []labels.Matcher, filter filter.Filter, mint, maxt int64, offset int, limit uint32) ([]*msg.Series, error) {
	var (
		series []*msg.Series
		mtx    sync.Mutex
		wg     sync.WaitGroup

		err error
	)

	var (
		seriesCount uint32
		recordCount uint32
	)

	ctx, cancel := context.WithTimeout(ctx, selTimeout)
	defer cancel()

	if parentSpan, ok := ctx.Value("span").(opentracing.Span); ok {
		outFor := opentracing.StartSpan("outFor", opentracing.ChildOf(parentSpan.Context()))
		defer func() {
			outFor.SetTag("seriesCount", seriesCount)
			outFor.SetTag("recordCount", recordCount)
			if err != nil {
				outFor.SetTag("err", err.Error())
			}
			outFor.Finish()
		}()
	}

	set, err := q.Select(matchers...)
	if err != nil {
		return nil, err
	}

	for set.Next() {
		select {
		case <-ctx.Done():
			return series, nil
		default:
		}

		seriesCount++

		curSeries := set.At()

		wg.Add(1)
		iterExecutor.Submit(func() {
			defer wg.Done()

			it := curSeries.Iterator()

			var (
				t       int64
				v       []byte
				points  msg.Points
				skipped int
			)

			if it.Seek(mint) {
				t, v = it.At()

				if t > maxt {
					return
				}

				if skipped < offset {
					skipped++
				} else if filter(v) {
					atomic.AddUint32(&recordCount, 1)
					vCopy := make([]byte, len(v))
					copy(vCopy, v)
					points = append(points, msg.Point{T: t, V: vCopy})
				}

				c, cl := context.WithCancel(ctx)
				defer cl()

				for (limit < 0 || atomic.LoadUint32(&recordCount) < limit) && it.Next() {
					select {
					case <-c.Done():
						level.Info(vars.Logger).Log("t", t, "v", v, "l", len(points))
						return
					default:
					}

					t, v = it.At()

					if t < mint {
						continue
					}

					if t > maxt {
						break
					}

					if skipped < offset {
						skipped++
						continue
					}

					if filter(v) {
						atomic.AddUint32(&recordCount, 1)
						vCopy := make([]byte, len(v))
						copy(vCopy, v)
						points = append(points, msg.Point{T: t, V: vCopy})
					}
				}

				s := &msg.Series{
					Labels: LabelsToProto(curSeries.Labels()),
					Points: points,
				}
				mtx.Lock()
				series = append(series, s)
				mtx.Unlock()
			}
		})
	}

	wg.Wait()

	return series, nil
}

type Storage struct {
	shardID string
	dbs     []*DB
	*AddReqHandler
	OpStat *OPStat
}

func New() (*Storage, error) {
	opStat := new(OPStat)

	var dbs []*DB

	opts := &Options{
		RetentionDuration:      uint64(vars.Cfg.Storage.TSDB.RetentionDuration) / 1e6,
		BlockRanges:            vars.Cfg.Storage.TSDB.BlockRanges,
		NoLockfile:             vars.Cfg.Storage.TSDB.NoLockfile,
		AllowOverlappingBlocks: true,
		CompactLowWaterMark:    uint64(vars.Cfg.Limit.Compact.LowWaterMark),
		CompactHighWaterMark:   uint64(vars.Cfg.Limit.Compact.HighWaterMark),
	}
	for _, path := range vars.Cfg.Storage.TSDB.Paths {
		db, err := Open(path, vars.Logger, nil, opts)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}

	return &Storage{
		dbs: dbs,
		AddReqHandler: &AddReqHandler{
			dbs:      dbs,
			opStat:   opStat,
			symbolsK: syn.NewMap(32),
			symbolsV: syn.NewMap(1 << 12),
			hashers: sync.Pool{New: func() interface{} {
				return util.NewHasher()
			}},
		},
		OpStat: opStat,
	}, nil
}

func (storage *Storage) Querier(mint, maxt int64) (Querier, error) {
	var queriers []Querier

	for _, db := range storage.dbs {
		q, err := db.Querier(mint, maxt)

		if err != nil {
			for _, querier := range queriers {
				querier.Close()
			}
			return nil, err
		}

		queriers = append(queriers, q)
	}

	return &querier{
		blocks: queriers,
	}, nil
}

func (storage *Storage) HandleSelectReq(request *backendmsg.SelectRequest) *backendmsg.SelectResponse {
	queryResponse := &backendmsg.SelectResponse{Status: msg.StatusCode_Failed}

	var span opentracing.Span
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.Binary, bytes.NewBuffer(request.SpanCtx))
	if err != nil {
		span = opentracing.StartSpan("storage_select")
	} else {
		span = opentracing.StartSpan("storage_select", opentracing.ChildOf(wireContext))
	}
	defer func() {
		if queryResponse.Status == msg.StatusCode_Succeed {
			atomic.AddUint64(&storage.opStat.SucceedSel, 1)
			span.SetTag("seriesNum", len(queryResponse.Series))
		} else {
			atomic.AddUint64(&storage.opStat.FailedSel, 1)
			span.SetTag("errorMsg", queryResponse.ErrorMsg)
		}
		span.Finish()
	}()

	if request.Mint > request.Maxt {
		queryResponse.ErrorMsg = "end time must not be before start time"
		return queryResponse
	}

	matchers, err := ProtoToMatchers(request.Matchers)
	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}

	filter, err := filter.MergeFilter(request.Filters)
	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}

	var (
		q      Querier
		series []*msg.Series
	)

	q, err = storage.Querier(request.Mint, request.Maxt)
	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}
	defer q.Close()

	ctx := context.WithValue(context.Background(), "span", span)

	if request.OnlyLabels {
		series, err = selectLabelsOnly(ctx, q, matchers)
	} else {
		series, err = selectSeries(ctx, q, matchers, filter, request.Mint, request.Maxt, request.Offset, uint32(request.Limit))
	}

	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}

	queryResponse.Status = msg.StatusCode_Succeed
	queryResponse.Series = series
	return queryResponse
}

func (storage *Storage) HandleLabelValuesReq(request *backendmsg.LabelValuesRequest) *msg.LabelValuesResponse {
	queryResponse := &msg.LabelValuesResponse{Status: msg.StatusCode_Failed}

	var span opentracing.Span
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.Binary, bytes.NewBuffer(request.SpanCtx))
	if err != nil {
		span = opentracing.StartSpan("storage_labelValues")
	} else {
		span = opentracing.StartSpan("storage_labelValues", opentracing.ChildOf(wireContext))
	}
	defer func() {
		if queryResponse.Status == msg.StatusCode_Succeed {
			atomic.AddUint64(&storage.opStat.SucceedLVals, 1)
			span.SetTag("valuesNum", len(queryResponse.Values))
		} else {
			atomic.AddUint64(&storage.opStat.FailedLVals, 1)
			span.SetTag("errorMsg", queryResponse.ErrorMsg)
		}
		span.Finish()
	}()

	q, err := storage.Querier(request.Mint, request.Maxt)
	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}
	defer q.Close()

	var values []string

	if len(request.Matchers) == 0 {
		values, err = q.LabelValues(request.Name)
	} else {
		queryResponse.ErrorMsg = "not implemented"
		return queryResponse
	}

	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}

	queryResponse.Status = msg.StatusCode_Succeed
	queryResponse.Values = values
	return queryResponse
}

func (storage *Storage) Close() error {
	var err error
	for _, db := range storage.dbs {
		multierr.Append(err, db.Close())
	}
	return err
}

func (storage *Storage) Info(detailed bool) (Stat, error) {
	stat := Stat{}

	var diskFree uint64 = math.MaxUint64
	for _, db := range storage.dbs {
		diskUsage, err := disk.Usage(db.Dir())
		if err != nil {
			return stat, err
		}

		if diskUsage.Free < diskFree {
			diskFree = diskUsage.Free
		}
	}

	stat.Node = meta.Node{
		ShardID:  storage.shardID,
		IP:       vars.LocalIP,
		Port:     vars.Cfg.TcpPort,
		DiskFree: uint64(math.Round(float64(diskFree) / vars.G)), //GB
	}

	if !detailed {
		return stat, nil
	}

	stat.OpStat = *storage.OpStat
	for _, db := range storage.dbs {
		stat.dbStats = append(stat.dbStats, &DBStat{
			SeriesNum:        db.Head().NumSeries(),
			BlockNum:         len(db.Blocks()),
			HeadMinTime:      db.Head().MinTime(),
			HeadMaxTime:      db.Head().MaxTime(),
			HeadMinValidTime: db.Head().MinValidTime(),
		})
	}

	return stat, nil
}

func (storage *Storage) JoinCluster(address string) (string, error) {
	if address == "" {
		localMeta, err := meta.LoadLocalMeta()
		if err != nil {
			if storage.shardID != "" {
				localMeta = meta.LocalMeta{storage.shardID}
			} else {
				localMeta = meta.LocalMeta{strings.Replace(uuid.NewV1().String(), "-", "", -1)}
			}
			meta.StoreLocalMeta(localMeta)
		}
		storage.shardID = localMeta.ShardID
	} else {
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

		localMeta := meta.LocalMeta{generalResp.Message}
		meta.StoreLocalMeta(localMeta)
		storage.shardID = generalResp.Message
	}

	return storage.shardID, nil
}

type AddReqHandler struct {
	dbs      []*DB
	opStat   *OPStat
	symbolsK *syn.Map
	symbolsV *syn.Map
	hashers  sync.Pool
}

func (addReqHandler *AddReqHandler) HandleAddReq(request *backendmsg.AddRequest) *msg.GeneralResponse {
	var (
		multiErr error
		cache    = make([]Appender, len(addReqHandler.dbs))
		hasher   = addReqHandler.hashers.Get().(*util.Hasher)
	)

	for _, series := range request.Series {
		var idx = (hasher.Hash(series.Labels) % 268435523) % uint64(len(addReqHandler.dbs))
		var ref uint64

		for _, p := range series.Points {
			var err error

			if ref != 0 {
				err = addReqHandler.getAppender(idx, &cache).AddFast(ref, p.T, p.V)
			} else {
				lset := make([]labels.Label, len(series.Labels))

				for i, lb := range series.Labels {
					if symbol, found := addReqHandler.symbolsK.Get(lb.Name); found {
						lset[i].Name = symbol.(string)
					} else {
						lset[i].Name = lb.Name
						addReqHandler.symbolsK.Set(lset[i].Name, lset[i].Name)
					}

					if symbol, found := addReqHandler.symbolsV.Get(lb.Value); found {
						lset[i].Value = symbol.(string)
					} else {
						lset[i].Value = lb.Value
						addReqHandler.symbolsV.Set(lset[i].Value, lset[i].Value)
					}
				}

				ref, err = addReqHandler.getAppender(idx, &cache).Add(lset, p.T, p.V)
			}

			atomic.AddUint64(&addReqHandler.opStat.ReceivedAdd, 1)
			if err == nil {
				atomic.AddUint64(&addReqHandler.opStat.SucceedAdd, 1)
			} else {
				atomic.AddUint64(&addReqHandler.opStat.FailedAdd, 1)
				switch err {
				case ErrAmendSample:
					atomic.AddUint64(&addReqHandler.opStat.AmendSample, 1)
					atomic.StoreInt64(&addReqHandler.opStat.LastAmendSample, p.T)
				case ErrOutOfBounds:
					atomic.AddUint64(&addReqHandler.opStat.OutOfBounds, 1)
					atomic.StoreInt64(&addReqHandler.opStat.LastOutOfBounds, p.T)
				default:
					multiErr = multierr.Append(multiErr, err)
				}
			}
		}
	}

	for _, app := range cache {
		if app != nil {
			if err := app.Commit(); err != nil {
				atomic.AddUint64(&addReqHandler.opStat.FailedCommit, 1)
				multiErr = multierr.Append(multiErr, err)
			}
		}
	}

	addReqHandler.hashers.Put(hasher)

	resp := &msg.GeneralResponse{
		Status: msg.StatusCode_Succeed,
	}

	if multiErr != nil {
		resp.Status = msg.StatusCode_Failed
		resp.Message = multiErr.Error()
	}
	return resp
}

func (addReqHandler *AddReqHandler) getAppender(idx uint64, apps *[]Appender) Appender {
	app := (*apps)[idx]
	if app == nil {
		app = addReqHandler.dbs[idx].Appender()
		(*apps)[idx] = app
	}
	return app
}
