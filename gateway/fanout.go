// Copyright 2017 The Prometheus Authors
// Copyright 2019 The Baudtime Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gateway

import (
	"container/heap"
	"context"
	"math"
	"strings"
	"sync"

	"github.com/baudb/baudb/datanode"
	"github.com/baudb/baudb/gateway/backend"
	"github.com/baudb/baudb/gateway/shardvisitor"
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/util/time"
	"github.com/baudb/baudb/vars"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"go.uber.org/multierr"
)

var shardsNotFound = errors.New("no shards is found")

type Fanout struct {
	localAPI *datanode.API
}

// NewFanout returns a new fan-out Backend, which proxies reads and writes
// through to multiple underlying storages.
func NewFanout(localAPI *datanode.API) *Fanout {
	return &Fanout{
		localAPI: localAPI,
	}
}

func (f *Fanout) Querier(ctx context.Context, mint, maxt int64) (backend.Querier, error) {
	return &fanoutQuerier{
		ctx:      ctx,
		mint:     mint,
		maxt:     maxt,
		localAPI: f.localAPI,
	}, nil
}

// StartTime implements the Backend interface.
func (f *Fanout) StartTime() (int64, error) {
	// StartTime of a fanout should be the earliest StartTime of all its storages,
	// both primary and secondaries.
	return int64(model.Latest), nil
}

// Close closes the storage and all its underlying resources.
func (f *Fanout) Close() (err error) {
	if f.localAPI != nil {
		err = f.localAPI.Close()
	}
	return
}

type fanoutQuerier struct {
	ctx        context.Context
	mint, maxt int64
	backend.Querier
	localAPI *datanode.API
}

func (q *fanoutQuerier) Select(params *backend.SelectParams, matchers ...*backendmsg.Matcher) (backend.SeriesSet, error) {
	shardIDs, err := meta.Router().GetShardIDsByTimeSpan(time.Time(q.mint), time.Time(q.maxt), matchers...)
	if err != nil {
		return emptySeriesSet, err
	}
	if len(shardIDs) == 0 {
		return emptySeriesSet, shardsNotFound
	}

	shardVisitor, found := shardvisitor.GetVisitor(vars.Cfg.Gateway.QueryStrategy)
	if !found {
		return emptySeriesSet, errors.Errorf("query strategy %v not found", vars.Cfg.Gateway.QueryStrategy)
	}

	queriers := make([]backend.Querier, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		if shardID == "" {
			continue
		}

		queriers = append(queriers, &querier{
			ctx:  q.ctx,
			mint: q.mint,
			maxt: q.maxt,
			client: &ShardClient{
				shardID:  shardID,
				localAPI: q.localAPI,
				exeQuery: shardVisitor,
			},
		})
	}

	q.Querier = NewMergeQuerier(queriers)
	return q.Querier.Select(params, matchers...)
}

func (q *fanoutQuerier) LabelValues(name string, matchers ...*backendmsg.Matcher) ([]string, error) {
	var shardIDs []string

	if q.mint < 0 || q.maxt == math.MaxInt64 {
		allShards := meta.AllShards()
		shardIDs = make([]string, 0, len(allShards))

		for shardID := range allShards {
			shardIDs = append(shardIDs, shardID)
		}
	} else {
		var err error
		shardIDs, err = meta.Router().GetShardIDsByTimeSpan(time.Time(q.mint), time.Time(q.maxt), matchers...)
		if err != nil {
			return nil, err
		}
	}
	if len(shardIDs) == 0 {
		return nil, shardsNotFound
	}

	shardVisitor, found := shardvisitor.GetVisitor(vars.Cfg.Gateway.QueryStrategy)
	if !found {
		return nil, errors.Errorf("query strategy %v not found", vars.Cfg.Gateway.QueryStrategy)
	}

	queriers := make([]backend.Querier, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		if shardID == "" {
			continue
		}

		queriers = append(queriers, &querier{
			ctx:  q.ctx,
			mint: q.mint,
			maxt: q.maxt,
			client: &ShardClient{
				shardID:  shardID,
				localAPI: q.localAPI,
				exeQuery: shardVisitor,
			},
		})
	}

	q.Querier = NewMergeQuerier(queriers)
	return q.Querier.LabelValues(name, matchers...)
}

func (q *fanoutQuerier) Close() error {
	if q.Querier != nil {
		return q.Querier.Close()
	}
	return nil
}

// mergeQuerier implements Querier.
type mergeQuerier struct {
	queriers []backend.Querier
}

// NewMergeQuerier returns a new Querier that merges results of input queriers.
// NB NewMergeQuerier will return NoopQuerier if no queriers are passed to it,
// and will filter NoopQueriers from its arguments, in order to reduce overhead
// when only one querier is passed.
func NewMergeQuerier(queriers []backend.Querier) backend.Querier {
	filtered := make([]backend.Querier, 0, len(queriers))
	for _, querier := range queriers {
		if querier != NoopQuerier() {
			filtered = append(filtered, querier)
		}
	}

	switch len(filtered) {
	case 0:
		return NoopQuerier()
	case 1:
		return filtered[0]
	default:
		return &mergeQuerier{
			queriers: filtered,
		}
	}
}

// Select returns a set of series that matches the given label matchers.
func (q *mergeQuerier) Select(params *backend.SelectParams, matchers ...*backendmsg.Matcher) (backend.SeriesSet, error) {
	var (
		errors     = make([]error, len(q.queriers))
		seriesSets = make([]backend.SeriesSet, len(q.queriers))
		wg         sync.WaitGroup
	)

	for i, querier := range q.queriers {
		wg.Add(1)
		go func(idx int, q backend.Querier) {
			defer wg.Done()

			seriesSets[idx], errors[idx] = q.Select(params, matchers...)
		}(i, querier)
	}
	wg.Wait()

	for i, sl := 0, len(seriesSets); i < sl; i++ {
		j := i - (sl - len(seriesSets))

		if seriesSets[j] == nil {
			seriesSets = append(seriesSets[:j], seriesSets[j+1:]...)
		}
	}

	return NewMergeSeriesSet(seriesSets), multierr.Combine(errors...)
}

// LabelValues returns all potential values for a label name.
func (q *mergeQuerier) LabelValues(name string, matchers ...*backendmsg.Matcher) ([]string, error) {
	var (
		errors = make([]error, len(q.queriers))
		values = make([][]string, len(q.queriers))
		wg     sync.WaitGroup
	)

	for i, querier := range q.queriers {
		wg.Add(1)
		go func(idx int, q backend.Querier) {
			defer wg.Done()

			values[idx], errors[idx] = q.LabelValues(name, matchers...)
		}(i, querier)
	}
	wg.Wait()

	multiErr := multierr.Combine(errors...)

	if multiErr != nil {
		return nil, multiErr
	}

	return mergeStringSlices(values), nil
}

func mergeStringSlices(ss [][]string) []string {
	switch len(ss) {
	case 0:
		return nil
	case 1:
		return ss[0]
	case 2:
		return mergeTwoStringSlices(ss[0], ss[1])
	default:
		halfway := len(ss) / 2
		return mergeTwoStringSlices(
			mergeStringSlices(ss[:halfway]),
			mergeStringSlices(ss[halfway:]),
		)
	}
}

func mergeTwoStringSlices(a, b []string) []string {
	i, j := 0, 0
	result := make([]string, 0, len(a)+len(b))
	for i < len(a) && j < len(b) {
		switch strings.Compare(a[i], b[j]) {
		case 0:
			result = append(result, a[i])
			i++
			j++
		case -1:
			result = append(result, a[i])
			i++
		case 1:
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// Close releases the resources of the Querier.
func (q *mergeQuerier) Close() error {
	// TODO return multiple errors?
	var lastErr error
	for _, querier := range q.queriers {
		if err := querier.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// mergeSeriesSet implements SeriesSet
type mergeSeriesSet struct {
	currentLabels msg.Labels
	currentSets   []backend.SeriesSet
	heap          seriesSetHeap
	sets          []backend.SeriesSet
}

// NewMergeSeriesSet returns a new series set that merges (deduplicates)
// series returned by the input series sets when iterating.
func NewMergeSeriesSet(sets []backend.SeriesSet) backend.SeriesSet {
	if len(sets) == 1 {
		return sets[0]
	}

	// Sets need to be pre-advanced, so we can introspect the label of the
	// series under the cursor.
	var h seriesSetHeap
	for _, set := range sets {
		if set.Next() {
			heap.Push(&h, set)
		}
	}
	return &mergeSeriesSet{
		heap: h,
		sets: sets,
	}
}

func (c *mergeSeriesSet) Next() bool {
	// Firstly advance all the current series sets.  If any of them have run out
	// we can drop them, otherwise they should be inserted back into the heap.
	for _, set := range c.currentSets {
		if set.Next() {
			heap.Push(&c.heap, set)
		}
	}
	if len(c.heap) == 0 {
		return false
	}

	// Now, pop items of the heap that have equal label sets.
	c.currentSets = nil
	c.currentLabels = c.heap[0].At().Labels()
	for len(c.heap) > 0 && msg.Equal(c.currentLabels, c.heap[0].At().Labels()) {
		set := heap.Pop(&c.heap).(backend.SeriesSet)
		c.currentSets = append(c.currentSets, set)
	}
	return true
}

func (c *mergeSeriesSet) At() backend.Series {
	if len(c.currentSets) == 1 {
		return c.currentSets[0].At()
	}
	var series []backend.Series
	for _, seriesSet := range c.currentSets {
		series = append(series, seriesSet.At())
	}
	return &mergeSeries{
		labels: c.currentLabels,
		series: series,
	}
}

func (c *mergeSeriesSet) Err() error {
	for _, set := range c.sets {
		if err := set.Err(); err != nil {
			return err
		}
	}
	return nil
}

type seriesSetHeap []backend.SeriesSet

func (h seriesSetHeap) Len() int      { return len(h) }
func (h seriesSetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesSetHeap) Less(i, j int) bool {
	a, b := h[i].At().Labels(), h[j].At().Labels()

	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if d := strings.Compare(a[i].Name, b[i].Name); d != 0 {
			return d < 0
		}
		if d := strings.Compare(a[i].Value, b[i].Value); d != 0 {
			return d < 0
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a)-len(b) < 0
}

func (h *seriesSetHeap) Push(x interface{}) {
	*h = append(*h, x.(backend.SeriesSet))
}

func (h *seriesSetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type mergeSeries struct {
	labels msg.Labels
	series []backend.Series
}

func (m *mergeSeries) Labels() msg.Labels {
	return m.labels
}

func (m *mergeSeries) Iterator() backend.SeriesIterator {
	iterators := make([]backend.SeriesIterator, 0, len(m.series))
	for _, s := range m.series {
		iterators = append(iterators, s.Iterator())
	}
	return newMergeIterator(iterators)
}

type mergeIterator struct {
	iterators []backend.SeriesIterator
	h         seriesIteratorHeap
}

func newMergeIterator(iterators []backend.SeriesIterator) backend.SeriesIterator {
	return &mergeIterator{
		iterators: iterators,
		h:         nil,
	}
}

func (c *mergeIterator) At() (t int64, v []byte) {
	if len(c.h) == 0 {
		panic("mergeIterator.At() called after .Next() returned false.")
	}

	// TODO do I need to dedupe or just merge?
	return c.h[0].At()
}

func (c *mergeIterator) Next() bool {
	if c.h == nil {
		for _, iter := range c.iterators {
			if iter.Next() {
				heap.Push(&c.h, iter)
			}
		}
		return len(c.h) > 0
	}

	if len(c.h) == 0 {
		return false
	}

	iter := heap.Pop(&c.h).(backend.SeriesIterator)
	if iter.Next() {
		heap.Push(&c.h, iter)
	}

	return len(c.h) > 0
}

func (c *mergeIterator) Err() error {
	for _, iter := range c.iterators {
		if err := iter.Err(); err != nil {
			return err
		}
	}
	return nil
}

type seriesIteratorHeap []backend.SeriesIterator

func (h seriesIteratorHeap) Len() int      { return len(h) }
func (h seriesIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesIteratorHeap) Less(i, j int) bool {
	at, _ := h[i].At()
	bt, _ := h[j].At()
	return at < bt
}

func (h *seriesIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(backend.SeriesIterator))
}

func (h *seriesIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (f *Fanout) Appender() (backend.Appender, error) {
	return &fanoutAppender{
		appenders: make(map[string]*appender),
		localAPI:  f.localAPI,
	}, nil
}

type fanoutAppender struct {
	appenders map[string]*appender
	localAPI  *datanode.API
}

func (fanoutApp *fanoutAppender) Add(l []msg.Label, t int64, v []byte, hash uint64) error {
	shardID, err := meta.Router().GetShardIDByLabels(time.Time(t), l, hash, true)
	if err != nil {
		return err
	}
	if shardID == "" {
		return shardsNotFound
	}

	app, found := fanoutApp.appenders[shardID]
	if !found {
		app, err = newAppender(shardID, fanoutApp.localAPI)
		if err != nil {
			return err
		}

		fanoutApp.appenders[shardID] = app
	}

	return app.Add(l, t, v, hash)
}

func (fanoutApp *fanoutAppender) Flush() error {
	var (
		multiErr error
		wg       sync.WaitGroup
		mtx      sync.Mutex
	)

	for _, app := range fanoutApp.appenders {
		wg.Add(1)
		go func(app *appender) {
			defer wg.Done()

			if err := app.Flush(); err != nil {
				mtx.Lock()
				multiErr = multierr.Append(multiErr, err)
				mtx.Unlock()
			}
		}(app)
	}
	wg.Wait()

	return multiErr
}
