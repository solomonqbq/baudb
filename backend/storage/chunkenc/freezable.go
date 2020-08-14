package chunkenc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
)

const (
	TimeWindowMs            = int64(5 * time.Minute / (time.Millisecond / time.Nanosecond))
	timeWindowPerSampleList = int64(30 * time.Millisecond / (time.Millisecond / time.Nanosecond))
	maxLenOfValue           = 15 * 1024
)

var (
	invalidChunkFormat = errors.New("invalid chunk format")
	invalidLenOfValue  = errors.New("invalid len of value")
	lz4Options         = []lz4.Option{lz4.BlockChecksumOption(false), lz4.ChecksumOption(false)}
)

type FreezableChunk struct {
	samples []*sampleList
	b       bstream
	frozen  bool
}

func NewFreezableChunk() *FreezableChunk {
	b := make([]byte, 4, 256)
	return &FreezableChunk{
		samples: make([]*sampleList, TimeWindowMs/timeWindowPerSampleList),
		b:       bstream{stream: b},
	}
}

func (c *FreezableChunk) Encoding() Encoding {
	return EncFrozen
}

func (c *FreezableChunk) Bytes() []byte {
	if !c.frozen {
		c.Freeze()
	}
	return c.b.bytes()
}

func (c *FreezableChunk) NumSamples() int {
	return int(binary.BigEndian.Uint32(c.b.bytes()))
}

func (c *FreezableChunk) Freeze() error {
	encBuf := make([]byte, binary.MaxVarintLen64)

	zw := getCompressWriter(&c.b)
	defer putCompressWriter(zw)

	var (
		t int64
		v []byte
	)

	iter := c.Iterator(nil)
	for iter.Next() {
		t, v = iter.At()

		zw.Write(encBuf[:binary.PutVarint(encBuf, t)])

		zw.Write(encBuf[:binary.PutUvarint(encBuf, uint64(len(v)))])
		zw.Write(v)
	}

	err := zw.Close()
	encBuf = nil

	if err != nil {
		b := c.b.bytes()
		b = b[:4]

		panic(err)

		return err
	}

	c.frozen = true
	c.samples = nil

	return nil
}

func (c *FreezableChunk) Appender() (Appender, error) {
	if c.frozen {
		return nil, errors.New("chunk is frozen")
	}
	return &treeAppender{
		b:       &c.b,
		samples: c.samples,
	}, nil
}

func (c *FreezableChunk) Iterator(it Iterator) Iterator {
	if c.frozen {
		compressIter, ok := it.(*compressIterator)
		if !ok {
			compressIter = &compressIterator{}
		}

		compressIter.Reset(c.b.bytes())
		return compressIter
	}

	treeIter, ok := it.(*treeIterator)
	if !ok {
		treeIter = &treeIterator{}
	}

	for i, l := range c.samples {
		if l != nil {
			treeIter.reset(binary.BigEndian.Uint32(c.b.bytes()), c.samples[i:])
			break
		}
	}

	return treeIter
}

type treeAppender struct {
	b       *bstream
	samples []*sampleList
}

func (a *treeAppender) Append(t int64, v []byte) {
	if len(v) == 0 {
		return
	}

	num := binary.BigEndian.Uint32(a.b.bytes())

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
	binary.BigEndian.PutUint32(a.b.bytes(), num+1)
}

type treeIterator struct {
	e       *element
	samples []*sampleList

	err error

	numTotal uint32
	numRead  uint32
}

func (it *treeIterator) reset(numTotal uint32, samples []*sampleList) {
	it.e = nil
	it.samples = samples
	it.err = nil
	it.numTotal = numTotal
	it.numRead = 0
}

func (it *treeIterator) At() (int64, []byte) {
	return it.e.Value.t, it.e.Value.v
}

func (it *treeIterator) Err() error {
	return it.err
}

func (it *treeIterator) Next() bool {
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

type compressIterator struct {
	zr *lz4.Reader
	r  *bufio.Reader

	t int64
	v []byte

	numTotal uint32
	numRead  uint32

	err error

	rdBuf []byte
}

func (it *compressIterator) Reset(b []byte) {
	br := newBReader(b[4:])

	if it.zr != nil {
		it.zr.Reset(br)
	} else {
		it.zr = lz4.NewReader(br)
	}

	if it.r != nil {
		it.r.Reset(it.zr)
	} else {
		it.r = bufio.NewReaderSize(it.zr, 256<<10)
	}

	it.t = 0
	it.v = nil
	it.err = nil

	it.numTotal = binary.BigEndian.Uint32(b)
	it.numRead = 0
}

func (it *compressIterator) At() (int64, []byte) {
	return it.t, it.v
}

func (it *compressIterator) Next() bool {
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}

	t, err := binary.ReadVarint(it.r)
	if err != nil {
		//panic(err)
		fmt.Println(err, it.numTotal, it.numRead)
		it.err = err
		return false
	}

	l, err := binary.ReadUvarint(it.r)
	if err != nil {
		//panic(err)
		fmt.Println(err, it.numTotal, it.numRead)
		it.err = err
		return false
	}

	if l > maxLenOfValue {
		it.err = invalidLenOfValue
		return false
	}

	if cap(it.rdBuf) < int(l) {
		it.rdBuf = make([]byte, l)
	}

	_, err = io.ReadFull(it.r, it.rdBuf[:l])
	if err != nil {
		//panic(err)
		fmt.Println(err, it.numTotal, it.numRead)
		it.err = err
		return false
	}

	it.numRead++

	it.t = t
	it.v = it.rdBuf[:l]
	return true
}

func (it *compressIterator) Err() error {
	return it.err
}

var zwPool = &sync.Pool{}

func getCompressWriter(dst io.Writer) *lz4.Writer {
	v := zwPool.Get()
	if v != nil {
		w := v.(*lz4.Writer)
		w.Reset(dst)
		return w
	}

	w := lz4.NewWriter(dst)
	w.Apply(lz4Options...)
	return w
}

func putCompressWriter(w *lz4.Writer) {
	w.Reset(nil)
	zwPool.Put(w)
}
