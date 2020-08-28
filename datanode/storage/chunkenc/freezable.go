package chunkenc

import (
	"sync/atomic"
	"time"
)

const (
	TimeWindowMs            = int64(5 * time.Minute / (time.Millisecond / time.Nanosecond))
	timeWindowPerSampleList = int64(30 * time.Millisecond / (time.Millisecond / time.Nanosecond))
)

type FreezableChunk struct {
	samples   []*sampleList
	size      uint32
	frozen    uint32
	frozenChk *CompressChunk
}

func NewFreezableChunk() *FreezableChunk {
	return &FreezableChunk{
		samples: make([]*sampleList, TimeWindowMs/timeWindowPerSampleList),
	}
}

func (c *FreezableChunk) Encoding() Encoding {
	return EncCompress
}

func (c *FreezableChunk) Bytes() []byte {
	if atomic.LoadUint32(&c.frozen) == 0 {
		c.Freeze()
	}
	return c.frozenChk.Bytes()
}

func (c *FreezableChunk) NumSamples() int {
	if atomic.LoadUint32(&c.frozen) == 0 {
		return int(c.size)
	}
	return c.frozenChk.NumSamples()
}

func (c *FreezableChunk) Freeze() error {
	if c.frozenChk == nil {
		c.frozenChk = NewCompressChunk()
	}

	app, err := c.frozenChk.CompressAppender()
	if err != nil {
		return err
	}

	iter := c.Iterator(nil)
	for iter.Next() {
		app.Append(iter.At())
	}
	app.Close()

	atomic.StoreUint32(&c.frozen, 1)

	c.size = 0
	c.samples = nil

	return nil
}

func (c *FreezableChunk) Appender() (Appender, error) {
	if atomic.LoadUint32(&c.frozen) == 1 {
		return c.frozenChk.Appender()
	}
	return &appenderBeforeFrozen{
		numSamples: &c.size,
		samples:    c.samples,
	}, nil
}

func (c *FreezableChunk) Iterator(it Iterator) Iterator {
	if atomic.LoadUint32(&c.frozen) == 1 {
		return c.frozenChk.Iterator(it)
	}

	iter, ok := it.(*iteratorBeforeFrozen)
	if !ok {
		iter = &iteratorBeforeFrozen{}
	}

	for i, l := range c.samples {
		if l != nil {
			iter.reset(c.size, c.samples[i:])
			break
		}
	}
	return iter
}

type appenderBeforeFrozen struct {
	numSamples *uint32
	samples    []*sampleList
}

func (a *appenderBeforeFrozen) Append(t int64, v []byte) {
	if len(v) == 0 {
		return
	}

	i := (t % TimeWindowMs) / timeWindowPerSampleList

	l := a.samples[i]
	if l == nil {
		l = newList()
		a.samples[i] = l
	}

	l.Insert(sample{
		t: t,
		v: v,
	})

	*a.numSamples += 1
}

type iteratorBeforeFrozen struct {
	e       *element
	samples []*sampleList

	err error

	numTotal uint32
	numRead  uint32
}

func (it *iteratorBeforeFrozen) reset(numTotal uint32, samples []*sampleList) {
	it.e = nil
	it.samples = samples
	it.err = nil
	it.numTotal = numTotal
	it.numRead = 0
}

func (it *iteratorBeforeFrozen) At() (int64, []byte) {
	return it.e.Value.t, it.e.Value.v
}

func (it *iteratorBeforeFrozen) Err() error {
	return it.err
}

func (it *iteratorBeforeFrozen) Next() bool {
	if it.err != nil || it.numRead >= it.numTotal {
		return false
	}

	if len(it.samples) == 0 {
		return false
	}

	var e *element
	if it.e == nil {
		e = it.samples[0].First()
	} else {
		e = it.e.Next()
	}

	if e != nil {
		it.e = e
		it.numRead++

		return true
	} //else next list

	if len(it.samples) == 1 { //no next list
		return false
	}

	samples := it.samples[1:]
	for i, l := range samples {
		if l != nil { //find next not-nil list
			it.e = nil
			it.samples = samples[i:]
			return it.Next()
		}
	}

	//no next list found
	it.samples = nil
	return false
}
