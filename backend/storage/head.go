// Copyright 2017 The Prometheus Authors
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

package storage

import (
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baudb/baudb/backend/storage/chunkenc"
	"github.com/baudb/baudb/backend/storage/chunks"
	"github.com/baudb/baudb/backend/storage/encoding"
	"github.com/baudb/baudb/backend/storage/index"
	"github.com/baudb/baudb/backend/storage/labels"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

var (
	// ErrNotFound is returned if a looked up resource was not found.
	ErrNotFound = errors.Errorf("not found")

	// ErrAmendSample is returned if an appended sample has the same timestamp
	// as the most recent sample but a different value.
	ErrAmendSample = errors.New("amending sample")

	// ErrOutOfBounds is returned if an appended sample is out of the
	// writable time range.
	ErrOutOfBounds = errors.New("out of bounds")

	// emptyTombstoneReader is a no-op Tombstone Reader.
	// This is used by head to satisfy the Tombstones() function call.
	emptyTombstoneReader = newMemTombstones()
)

// RefSeries is the series labels with the series ID.
type RefSeries struct {
	Ref    uint64
	Labels labels.Labels
}

// RefSample is a timestamp/value pair associated with a reference to a series.
type RefSample struct {
	Ref uint64
	T   int64
	V   []byte

	series *memSeries
}

// Head handles reads and writes of time series data within a time window.
type Head struct {
	pendingReaders sync.WaitGroup
	pendingWriters sync.WaitGroup

	chunkRange int64
	metrics    *headMetrics
	logger     log.Logger
	appendPool sync.Pool
	bytesPool  sync.Pool
	numSeries  uint64

	minTime, maxTime int64 // Current min and max of the samples included in the head.
	lastSeriesID     uint64

	// All series addressable by their ID or hash.
	series *stripeSeries

	symMtx  sync.RWMutex
	symbols map[string]struct{}
	values  map[string]stringset // label names to possible values

	postings *index.MemPostings // postings lists for terms
}

type headMetrics struct {
	activeAppenders   int64
	seriesCreated     uint64
	seriesRemoved     uint64
	seriesNotFound    uint64
	chunks            uint64
	chunksCreated     uint64
	chunksRemoved     uint64
	gcDuration        [3]float64
	bytes             uint64
	samplesAppended   uint64
	headTruncateFail  uint64
	headTruncateTotal uint64
}

// NewHead opens the head block in dir.
func NewHead(l log.Logger, chunkRange int64) (*Head, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	if chunkRange < 1 {
		return nil, errors.Errorf("invalid chunk range %d", chunkRange)
	}
	h := &Head{
		logger:     l,
		chunkRange: chunkRange,
		minTime:    math.MaxInt64,
		maxTime:    math.MinInt64,
		series:     newStripeSeries(),
		values:     map[string]stringset{},
		symbols:    map[string]struct{}{},
		postings:   index.NewUnorderedMemPostings(),
	}
	h.metrics = &headMetrics{}

	return h, nil
}

func (h *Head) updateMinMaxTime(mint, maxt int64) {
	for {
		lt := h.MinTime()
		if mint >= lt {
			break
		}
		if atomic.CompareAndSwapInt64(&h.minTime, lt, mint) {
			break
		}
	}
	for {
		ht := h.MaxTime()
		if maxt <= ht {
			break
		}
		if atomic.CompareAndSwapInt64(&h.maxTime, ht, maxt) {
			break
		}
	}
}

// Init loads data from the write ahead log and prepares the head for writes.
// It should be called before using an appender so that
// limits the ingested samples to the head min valid time.
func (h *Head) Init() error {
	defer h.postings.EnsureOrder()
	defer h.gc() // After loading the wal remove the obsolete data from the head.

	return nil
}

// Truncate removes old data before mint from the head.
func (h *Head) Reset() (err error) {
	defer func() {
		if err != nil {
			atomic.AddUint64(&h.metrics.headTruncateFail, 1)
		}
	}()

	atomic.StoreInt64(&h.minTime, math.MaxInt64)

	atomic.AddUint64(&h.metrics.headTruncateTotal, 1)
	start := time.Now()

	h.gc()
	level.Info(h.logger).Log("msg", "head GC completed", "duration", time.Since(start))

	atomic.StoreUint64(&h.metrics.bytes, 0)

	h.metrics.gcDuration[0] = h.metrics.gcDuration[1]
	h.metrics.gcDuration[1] = h.metrics.gcDuration[2]
	h.metrics.gcDuration[2] = time.Since(start).Seconds()

	return nil
}

// initTime initializes a head with the first timestamp. This only needs to be called
// for a completely fresh head with an empty WAL.
// Returns true if the initialization took an effect.
func (h *Head) initTime(t int64) (initialized bool) {
	if !atomic.CompareAndSwapInt64(&h.minTime, math.MaxInt64, t) {
		return false
	}
	// Ensure that max time is initialized to at least the min time we just set.
	// Concurrent appenders may already have set it to a higher value.
	atomic.CompareAndSwapInt64(&h.maxTime, math.MinInt64, t)

	return true
}

type rangeHead struct {
	head       *Head
	mint, maxt int64
}

func (h *rangeHead) Index() (IndexReader, error) {
	return h.head.indexRange(h.mint, h.maxt), nil
}

func (h *rangeHead) Chunks() (ChunkReader, error) {
	return h.head.chunksRange(h.mint, h.maxt), nil
}

func (h *rangeHead) Tombstones() (TombstoneReader, error) {
	return emptyTombstoneReader, nil
}

func (h *rangeHead) MinTime() int64 {
	return h.mint
}

func (h *rangeHead) MaxTime() int64 {
	return h.maxt
}

func (h *rangeHead) NumSeries() uint64 {
	return h.head.NumSeries()
}

func (h *rangeHead) Meta() BlockMeta {
	return BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    h.head.Meta().ULID,
		Stats: BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// initAppender is a helper to initialize the time bounds of the head
// upon the first sample it receives.
type initAppender struct {
	app  Appender
	head *Head
}

func (a *initAppender) Add(lset labels.Labels, t int64, v []byte) (uint64, error) {
	if a.app != nil {
		return a.app.Add(lset, t, v)
	}
	a.head.initTime(t)
	a.app = a.head.appender()

	return a.app.Add(lset, t, v)
}

func (a *initAppender) AddFast(ref uint64, t int64, v []byte) error {
	if a.app == nil {
		return ErrNotFound
	}
	return a.app.AddFast(ref, t, v)
}

func (a *initAppender) Commit() error {
	if a.app == nil {
		return nil
	}
	return a.app.Commit()
}

func (a *initAppender) Rollback() error {
	if a.app == nil {
		return nil
	}
	return a.app.Rollback()
}

// Appender returns a new Appender on the database.
func (h *Head) Appender() Appender {
	atomic.AddInt64(&h.metrics.activeAppenders, 1)

	// The head cache might not have a starting point yet. The init appender
	// picks up the first appended timestamp as the base.
	if h.MinTime() == math.MaxInt64 {
		return &initAppender{head: h}
	}
	return h.appender()
}

func (h *Head) appender() *headAppender {
	h.pendingWriters.Add(1)
	return &headAppender{
		head: h,
		// Set the minimum valid time to whichever is greater the head min valid time or the compaciton window.
		// This ensures that no samples will be added within the compaction window to avoid races.
		mint:    math.MaxInt64,
		maxt:    math.MinInt64,
		samples: h.getAppendBuffer(),
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (h *Head) getAppendBuffer() []RefSample {
	b := h.appendPool.Get()
	if b == nil {
		return make([]RefSample, 0, 512)
	}
	return b.([]RefSample)
}

func (h *Head) putAppendBuffer(b []RefSample) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.appendPool.Put(b[:0])
}

func (h *Head) getBytesBuffer() []byte {
	b := h.bytesPool.Get()
	if b == nil {
		return make([]byte, 0, 1024)
	}
	return b.([]byte)
}

func (h *Head) putBytesBuffer(b []byte) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.bytesPool.Put(b[:0])
}

type headAppender struct {
	head       *Head
	mint, maxt int64

	series  []RefSeries
	samples []RefSample
}

func (a *headAppender) Add(lset labels.Labels, t int64, v []byte) (uint64, error) {
	// Ensure no empty labels have gotten through.
	lset = lset.WithoutEmpty()

	s, created := a.head.getOrCreate(lset.Hash(), lset)
	if created {
		a.series = append(a.series, RefSeries{
			Ref:    s.ref,
			Labels: lset,
		})
	}
	return s.ref, a.AddFast(s.ref, t, v)
}

func (a *headAppender) AddFast(ref uint64, t int64, v []byte) error {
	s := a.head.series.getByID(ref)
	if s == nil {
		return errors.Wrap(ErrNotFound, "unknown series")
	}
	s.Lock()
	if err := s.appendable(t, v); err != nil {
		s.Unlock()
		return err
	}
	s.pendingCommit = true
	s.Unlock()

	if t < a.mint {
		a.mint = t
	}
	if t > a.maxt {
		a.maxt = t
	}

	a.samples = append(a.samples, RefSample{
		Ref:    ref,
		T:      t,
		V:      v,
		series: s,
	})
	return nil
}

func (a *headAppender) Commit() error {
	defer a.head.pendingWriters.Done()
	defer atomic.AddInt64(&a.head.metrics.activeAppenders, -1)
	defer a.head.putAppendBuffer(a.samples)

	total := len(a.samples)
	bytesAppended := 0

	for _, s := range a.samples {
		s.series.Lock()
		ok, chunkCreated := s.series.append(s.T, s.V)
		s.series.pendingCommit = false
		s.series.Unlock()

		if !ok {
			total--
		} else {
			bytesAppended += len(s.V)
		}

		if chunkCreated {
			atomic.AddUint64(&a.head.metrics.chunks, 1)
			atomic.AddUint64(&a.head.metrics.chunksCreated, 1)
		}
	}

	atomic.AddUint64(&a.head.metrics.bytes, uint64(bytesAppended))
	atomic.AddUint64(&a.head.metrics.samplesAppended, uint64(total))
	a.head.updateMinMaxTime(a.mint, a.maxt)

	return nil
}

func (a *headAppender) Rollback() error {
	defer a.head.pendingWriters.Done()
	defer atomic.AddInt64(&a.head.metrics.activeAppenders, -1)
	for _, s := range a.samples {
		s.series.Lock()
		s.series.pendingCommit = false
		s.series.Unlock()
	}
	a.head.putAppendBuffer(a.samples)

	// Series are created in the head memory regardless of rollback. Thus we have
	// to log them to the WAL in any case.
	a.samples = nil
	return nil
}

// Delete all samples in the range of [mint, maxt] for series that satisfy the given
// label matchers.
func (h *Head) Delete(mint, maxt int64, ms ...labels.Matcher) error {
	// Do not delete anything beyond the currently valid range.
	mint, maxt = clampInterval(mint, maxt, h.MinTime(), h.MaxTime())

	ir := h.indexRange(mint, maxt)

	p, err := PostingsForMatchers(ir, ms...)
	if err != nil {
		return errors.Wrap(err, "select series")
	}

	dirty := false
	for p.Next() {
		series := h.series.getByID(p.At())

		t0, t1 := series.minTime(), series.maxTime()
		if t0 == math.MinInt64 || t1 == math.MinInt64 {
			continue
		}
		// Delete only until the current values and not beyond.
		t0, t1 = clampInterval(mint, maxt, t0, t1)
		if err := h.chunkRewrite(p.At(), Intervals{{t0, t1}}); err != nil {
			return errors.Wrap(err, "delete samples")
		}
		dirty = true
	}
	if p.Err() != nil {
		return p.Err()
	}

	if dirty {
		h.gc()
	}

	return nil
}

// chunkRewrite re-writes the chunks which overlaps with deleted ranges
// and removes the samples in the deleted ranges.
// Chunks is deleted if no samples are left at the end.
func (h *Head) chunkRewrite(ref uint64, dranges Intervals) (err error) {
	if len(dranges) == 0 {
		return nil
	}

	ms := h.series.getByID(ref)
	ms.Lock()
	defer ms.Unlock()
	if len(ms.chunks) == 0 {
		return nil
	}

	metas := ms.chunksMetas()
	mint, maxt := metas[0].MinTime, metas[len(metas)-1].MaxTime
	it := newChunkSeriesIterator(nil, metas, dranges, mint, maxt)

	ms.reset()
	for it.Next() {
		t, v := it.At()
		ok, _ := ms.append(t, v)
		if !ok {
			level.Warn(h.logger).Log("msg", "failed to add sample during delete")
		}
	}

	return nil
}

// gc removes data before the minimum timestamp from the head.
func (h *Head) gc() {
	h.pendingReaders.Wait()

	// Only data strictly lower than this timestamp must be deleted.
	mint := h.MinTime()

	// Drop old chunks and remember series IDs and hashes if they can be
	// deleted entirely.
	deleted, chunksRemoved := h.series.gc(mint)
	seriesRemoved := len(deleted)

	atomic.AddUint64(&h.metrics.seriesRemoved, uint64(seriesRemoved))
	atomic.AddUint64(&h.metrics.chunksRemoved, uint64(chunksRemoved))
	atomic.AddUint64(&h.metrics.chunks, ^uint64(chunksRemoved-1))
	// Using AddUint64 to substract series removed.
	// See: https://golang.org/pkg/sync/atomic/#AddUint64.
	atomic.AddUint64(&h.numSeries, ^uint64(seriesRemoved-1))

	// Remove deleted series IDs from the postings lists.
	h.postings.Delete(deleted)

	// Rebuild symbols and label value indices from what is left in the postings terms.
	symbols := make(map[string]struct{}, len(h.symbols))
	values := make(map[string]stringset, len(h.values))

	if err := h.postings.Iter(func(t labels.Label, _ index.Postings) error {
		symbols[t.Name] = struct{}{}
		symbols[t.Value] = struct{}{}

		ss, ok := values[t.Name]
		if !ok {
			ss = stringset{}
			values[t.Name] = ss
		}
		ss.set(t.Value)
		return nil
	}); err != nil {
		// This should never happen, as the iteration function only returns nil.
		panic(err)
	}

	h.symMtx.Lock()

	h.symbols = symbols
	h.values = values

	h.symMtx.Unlock()
}

// Tombstones returns a new reader over the head's tombstones
func (h *Head) Tombstones() (TombstoneReader, error) {
	return emptyTombstoneReader, nil
}

// Index returns an IndexReader against the block.
func (h *Head) Index() (IndexReader, error) {
	return h.indexRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) indexRange(mint, maxt int64) *headIndexReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	h.pendingReaders.Add(1)
	return &headIndexReader{head: h, mint: mint, maxt: maxt}
}

// Chunks returns a ChunkReader against the block.
func (h *Head) Chunks() (ChunkReader, error) {
	return h.chunksRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) chunksRange(mint, maxt int64) *headChunkReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	h.pendingReaders.Add(1)
	return &headChunkReader{head: h, mint: mint, maxt: maxt}
}

// NumSeries returns the number of active series in the head.
func (h *Head) NumSeries() uint64 {
	return atomic.LoadUint64(&h.numSeries)
}

// Meta returns meta information about the head.
// The head is dynamic so will return dynamic results.
func (h *Head) Meta() BlockMeta {
	var id [16]byte
	copy(id[:], "______head______")
	return BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    ulid.ULID(id),
		Stats: BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// MinTime returns the lowest time bound on visible data in the head.
func (h *Head) MinTime() int64 {
	return atomic.LoadInt64(&h.minTime)
}

// MaxTime returns the highest timestamp seen in data of the head.
func (h *Head) MaxTime() int64 {
	return atomic.LoadInt64(&h.maxTime)
}

func (h *Head) MinValidTime() int64 {
	return atomic.LoadInt64(&h.minValidTime)
}

// compactable returns whether the head has a compactable range.
// The head has a compactable range when the head time range is 1.5 times the chunk range.
// The 0.5 acts as a buffer of the appendable window.
func (h *Head) compactable() bool {
	bytesTotal := atomic.LoadUint64(&h.metrics.bytes)
	return h.MaxTime()-h.MinTime() > h.chunkRange && bytesTotal > 3000000000
}

// Close flushes the WAL and closes the head.
func (h *Head) Close() error {
	return nil
}

type headChunkReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headChunkReader) Close() error {
	h.head.pendingReaders.Done()
	return nil
}

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panicks if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
func packChunkID(seriesID, chunkID uint64) uint64 {
	if seriesID > (1<<40)-1 {
		panic("series ID exceeds 5 bytes")
	}
	if chunkID > (1<<24)-1 {
		panic("chunk ID exceeds 3 bytes")
	}
	return (seriesID << 24) | chunkID
}

func unpackChunkID(id uint64) (seriesID, chunkID uint64) {
	return id >> 24, (id << 40) >> 40
}

// Chunk returns the chunk for the reference number.
func (h *headChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	sid, cid := unpackChunkID(ref)

	s := h.head.series.getByID(sid)
	// This means that the series has been garbage collected.
	if s == nil {
		return nil, ErrNotFound
	}

	s.Lock()
	c := s.chunk(int(cid))

	// This means that the chunk has been garbage collected or is outside
	// the specified range.
	if c == nil || !c.OverlapsClosedInterval(h.mint, h.maxt) {
		s.Unlock()
		return nil, ErrNotFound
	}
	s.Unlock()

	return &safeChunk{
		Chunk: c.chunk,
		s:     s,
		cid:   int(cid),
	}, nil
}

type safeChunk struct {
	chunkenc.Chunk
	s   *memSeries
	cid int
}

func (c *safeChunk) Iterator(reuseIter chunkenc.Iterator) chunkenc.Iterator {
	c.s.Lock()
	it := c.s.iterator(c.cid, reuseIter)
	c.s.Unlock()
	return it
}

type headIndexReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headIndexReader) Close() error {
	h.head.pendingReaders.Done()
	return nil
}

func (h *headIndexReader) Symbols() (map[string]struct{}, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()

	res := make(map[string]struct{}, len(h.head.symbols))

	for s := range h.head.symbols {
		res[s] = struct{}{}
	}
	return res, nil
}

// LabelValues returns the possible label values
func (h *headIndexReader) LabelValues(names ...string) (index.StringTuples, error) {
	if len(names) != 1 {
		return nil, encoding.ErrInvalidSize
	}

	h.head.symMtx.RLock()
	sl := make([]string, 0, len(h.head.values[names[0]]))
	for s := range h.head.values[names[0]] {
		sl = append(sl, s)
	}
	h.head.symMtx.RUnlock()
	sort.Strings(sl)

	return index.NewStringTuples(sl, len(names))
}

// LabelNames returns all the unique label names present in the head.
func (h *headIndexReader) LabelNames() ([]string, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()
	labelNames := make([]string, 0, len(h.head.values))
	for name := range h.head.values {
		if name == "" {
			continue
		}
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames, nil
}

// Postings returns the postings list iterator for the label pair.
func (h *headIndexReader) Postings(name, value string) (index.Postings, error) {
	return h.head.postings.Get(name, value), nil
}

func (h *headIndexReader) SortedPostings(p index.Postings) index.Postings {
	series := make([]*memSeries, 0, 128)

	// Fetch all the series only once.
	for p.Next() {
		s := h.head.series.getByID(p.At())
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "looked up series not found")
		} else {
			series = append(series, s)
		}
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(errors.Wrap(err, "expand postings"))
	}

	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].lset, series[j].lset) < 0
	})

	// Convert back to list.
	ep := make([]uint64, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

// Series returns the series for the given reference.
func (h *headIndexReader) Series(ref uint64, lbls *labels.Labels, chks *[]chunks.Meta) error {
	s := h.head.series.getByID(ref)

	if s == nil {
		atomic.AddUint64(&h.head.metrics.seriesNotFound, 1)
		return ErrNotFound
	}
	*lbls = append((*lbls)[:0], s.lset...)

	s.Lock()
	defer s.Unlock()

	*chks = (*chks)[:0]

	for i, c := range s.chunks {
		// Do not expose chunks that are outside of the specified range.
		if !c.OverlapsClosedInterval(h.mint, h.maxt) {
			continue
		}
		// Set the head chunks as open (being appended to).
		maxTime := c.maxTime
		if s.headChunk == c {
			maxTime = math.MaxInt64
		}

		*chks = append(*chks, chunks.Meta{
			MinTime: c.minTime,
			MaxTime: maxTime,
			Ref:     packChunkID(s.ref, uint64(s.chunkID(i))),
		})
	}

	return nil
}

func (h *headIndexReader) LabelIndices() ([][]string, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()
	res := [][]string{}
	for s := range h.head.values {
		res = append(res, []string{s})
	}
	return res, nil
}

func (h *Head) getOrCreate(hash uint64, lset labels.Labels) (*memSeries, bool) {
	// Just using `getOrSet` below would be semantically sufficient, but we'd create
	// a new series on every sample inserted via Add(), which causes allocations
	// and makes our series IDs rather random and harder to compress in postings.
	s := h.series.getByHash(hash, lset)
	if s != nil {
		return s, false
	}

	// Optimistically assume that we are the first one to create the series.
	id := atomic.AddUint64(&h.lastSeriesID, 1)

	return h.getOrCreateWithID(id, hash, lset)
}

func (h *Head) getOrCreateWithID(id, hash uint64, lset labels.Labels) (*memSeries, bool) {
	s := newMemSeries(lset, id)

	s, created := h.series.getOrSet(hash, s)
	if !created {
		return s, false
	}

	atomic.AddUint64(&h.metrics.seriesCreated, 1)
	atomic.AddUint64(&h.numSeries, 1)

	h.postings.Add(id, lset)

	h.symMtx.Lock()
	defer h.symMtx.Unlock()

	for _, l := range lset {
		valset, ok := h.values[l.Name]
		if !ok {
			valset = stringset{}
			h.values[l.Name] = valset
		}
		valset.set(l.Value)

		h.symbols[l.Name] = struct{}{}
		h.symbols[l.Value] = struct{}{}
	}

	return s, true
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is built
// on top of a regular hashmap and holds a slice of series to resolve hash collisions.
// Its methods require the hash to be submitted with it to avoid re-computations throughout
// the code.
type seriesHashmap map[uint64][]*memSeries

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *memSeries {
	for _, s := range m[hash] {
		if s.lset.Equals(lset) {
			return s
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *memSeries) {
	l := m[hash]
	for i, prev := range l {
		if prev.lset.Equals(s.lset) {
			l[i] = s
			return
		}
	}
	m[hash] = append(l, s)
}

func (m seriesHashmap) del(hash uint64, lset labels.Labels) {
	var rem []*memSeries
	for _, s := range m[hash] {
		if !s.lset.Equals(lset) {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
type stripeSeries struct {
	series [stripeSize]map[uint64]*memSeries
	hashes [stripeSize]seriesHashmap
	locks  [stripeSize]stripeLock
}

const (
	stripeSize = 1 << 14
	stripeMask = stripeSize - 1
)

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

func newStripeSeries() *stripeSeries {
	s := &stripeSeries{}

	for i := range s.series {
		s.series[i] = map[uint64]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left.
func (s *stripeSeries) gc(mint int64) (map[uint64]struct{}, int) {
	var (
		deleted  = map[uint64]struct{}{}
		rmChunks = 0
	)
	// Run through all series and truncate old chunks. Mark those with no
	// chunks left as deleted and store their ID.
	for i := 0; i < stripeSize; i++ {
		s.locks[i].Lock()

		for hash, all := range s.hashes[i] {
			for _, series := range all {
				series.Lock()
				rmChunks += series.truncateChunksBefore(mint)

				if len(series.chunks) > 0 || series.pendingCommit {
					series.Unlock()
					continue
				}

				// The series is gone entirely. We need to keep the series lock
				// and make sure we have acquired the stripe locks for hash and ID of the
				// series alike.
				// If we don't hold them all, there's a very small chance that a series receives
				// samples again while we are half-way into deleting it.
				j := int(series.ref & stripeMask)

				if i != j {
					s.locks[j].Lock()
				}

				deleted[series.ref] = struct{}{}
				s.hashes[i].del(hash, series.lset)
				delete(s.series[j], series.ref)

				if i != j {
					s.locks[j].Unlock()
				}

				series.Unlock()
			}
		}

		s.locks[i].Unlock()
	}

	return deleted, rmChunks
}

func (s *stripeSeries) getByID(id uint64) *memSeries {
	i := id & stripeMask

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	i := hash & stripeMask

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getOrSet(hash uint64, series *memSeries) (*memSeries, bool) {
	i := hash & stripeMask

	s.locks[i].Lock()

	if prev := s.hashes[i].get(hash, series.lset); prev != nil {
		s.locks[i].Unlock()
		return prev, false
	}
	s.hashes[i].set(hash, series)
	s.locks[i].Unlock()

	i = series.ref & stripeMask

	s.locks[i].Lock()
	s.series[i][series.ref] = series
	s.locks[i].Unlock()

	return series, true
}

type sample struct {
	t int64
	v []byte
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() []byte {
	return s.v
}

// memSeries is the in-memory representation of a series. None of its methods
// are goroutine safe and it is the caller's responsibility to lock it.
type memSeries struct {
	sync.Mutex

	ref          uint64
	lset         labels.Labels
	chunks       []*memChunk
	headChunk    *memChunk
	baseTime     int64
	firstChunkID int

	pendingCommit bool // Whether there are samples waiting to be committed to this series.
}

func newMemSeries(lset labels.Labels, id uint64) *memSeries {
	s := &memSeries{
		lset:     lset,
		ref:      id,
		baseTime: math.MaxInt64,
	}
	return s
}

func (s *memSeries) minTime() int64 {
	if len(s.chunks) == 0 {
		return math.MinInt64
	}
	return s.chunks[0].minTime
}

func (s *memSeries) maxTime() int64 {
	c := s.head()
	if c == nil {
		return math.MinInt64
	}
	return c.maxTime
}

func (s *memSeries) chunksMetas() []chunks.Meta {
	metas := make([]chunks.Meta, 0, len(s.chunks))
	for _, chk := range s.chunks {
		metas = append(metas, chunks.Meta{Chunk: chk.chunk, MinTime: chk.minTime, MaxTime: chk.maxTime})
	}
	return metas
}

// reset re-initialises all the variable in the memSeries except 'lset', 'ref',
// and 'chunkRange', like how it would appear after 'newMemSeries(...)'.
func (s *memSeries) reset() {
	s.chunks = nil
	s.headChunk = nil
	s.firstChunkID = 0
	s.pendingCommit = false
}

// appendable checks whether the given sample is valid for appending to the series.
func (s *memSeries) appendable(t int64, v interface{}) error {
	if s.baseTime == math.MaxInt64 {
		s.baseTime = getBaseTime()
	}

	if t < s.baseTime {
		return ErrOutOfBounds
	}

	idx := (t - s.baseTime) / chunkenc.TimeWindowMs
	if idx > 42 {
		return ErrOutOfBounds
	}
	return nil
}

func (s *memSeries) chunk(id int) *memChunk {
	ix := id - s.firstChunkID
	if ix < 0 || ix >= len(s.chunks) {
		return nil
	}
	return s.chunks[ix]
}

func (s *memSeries) chunkID(pos int) int {
	return pos + s.firstChunkID
}

// truncateChunksBefore removes all chunks from the series that have not timestamp
// at or after mint. Chunk IDs remain unchanged.
func (s *memSeries) truncateChunksBefore(mint int64) (removed int) {
	var k int
	for i, c := range s.chunks {
		if c.minValidTime >= mint {
			break
		}
		k = i + 1
	}
	s.chunks = append(s.chunks[:0], s.chunks[k:]...)
	s.firstChunkID += k
	if len(s.chunks) == 0 {
		s.headChunk = nil
	} else {
		s.headChunk = s.chunks[len(s.chunks)-1]
	}
	s.baseTime = math.MaxInt64
	return k
}

// append adds the sample (t, v) to the series.
func (s *memSeries) append(t int64, v []byte) (success, chunkCreated bool) {
	idx := (t - s.baseTime) / chunkenc.TimeWindowMs

	i := int64(len(s.chunks) - 1)
	for i < idx {
		i++
		minT := s.baseTime + i*chunkenc.TimeWindowMs

		c := &memChunk{
			chunk:        chunkenc.NewFreezableChunk(),
			minTime:      math.MaxInt64,
			maxTime:      math.MinInt64,
			minValidTime: minT,
			maxValidTime: minT + chunkenc.TimeWindowMs,
		}

		app, err := c.chunk.Appender()
		if err != nil {
			panic(err)
		}
		c.app = app

		s.chunks = append(s.chunks, c)
		s.headChunk = c

		chunkCreated = true
	}

	c := s.chunks[idx]
	c.app.Append(t, v)

	if t > c.maxTime {
		c.maxTime = t
	}

	if t < c.minTime {
		c.minTime = t
	}

	return true, chunkCreated
}

// computeChunkEndTime estimates the end timestamp based the beginning of a chunk,
// its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
func computeChunkEndTime(start, cur, max int64) int64 {
	a := (max - start) / ((cur - start + 1) * 4)
	if a == 0 {
		return max
	}
	return start + (max-start)/a
}

func (s *memSeries) iterator(id int, it chunkenc.Iterator) chunkenc.Iterator {
	c := s.chunk(id)
	// TODO(fabxc): Work around! A querier may have retrieved a pointer to a series' chunk,
	// which got then garbage collected before it got accessed.
	// We must ensure to not garbage collect as long as any readers still hold a reference.
	if c == nil {
		return chunkenc.NewNopIterator()
	}

	return c.chunk.Iterator(it)
}

func (s *memSeries) head() *memChunk {
	return s.headChunk
}

type memChunk struct {
	chunk                      chunkenc.Chunk
	minTime, maxTime           int64
	minValidTime, maxValidTime int64
	app                        chunkenc.Appender
}

// Returns true if the chunk overlaps [mint, maxt].
func (mc *memChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return mc.minTime <= maxt && mint <= mc.maxTime
}

type memSafeIterator struct {
	chunkenc.Iterator

	i     int
	total int
	buf   [4]sample
}

func (it *memSafeIterator) Next() bool {
	if it.i+1 >= it.total {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *memSafeIterator) At() (int64, []byte) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.buf[4-(it.total-it.i)]
	return s.t, s.v
}

type stringset map[string]struct{}

func (ss stringset) set(s string) {
	ss[s] = struct{}{}
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}

func getBaseTime() int64 {
	t := time.Now().Add(-3 * time.Hour)
	ts := t.Unix()*1000 + int64(t.Nanosecond())/int64(time.Millisecond)
	return (ts / chunkenc.TimeWindowMs) * chunkenc.TimeWindowMs
}
