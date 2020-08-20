package chunkenc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/pierrec/lz4/v4"
	"io"
	"sync"
)

var (
	lz4Options        = []lz4.Option{lz4.BlockChecksumOption(false), lz4.ChecksumOption(false)}
)

type CompressChunk struct {
	b bstream
}

func NewCompressChunk() *CompressChunk {
	b := make([]byte, 4, 256)
	return &CompressChunk{
		b: bstream{stream: b},
	}
}

func (c *CompressChunk) Encoding() Encoding {
	return EncCompress
}

func (c *CompressChunk) Bytes() []byte {
	return c.b.bytes()
}

func (c *CompressChunk) NumSamples() int {
	return int(binary.BigEndian.Uint32(c.b.bytes()))
}

func (c *CompressChunk) Appender() (Appender, error) {
	return nil, errors.New("not support, chunk is frozen")
}

func (c *CompressChunk) CompressAppender() (AppendCloser, error) {
	return &compressAppender{
		b:      &c.b,
		encBuf: make([]byte, binary.MaxVarintLen64),
	}, nil
}

func (c *CompressChunk) Iterator(it Iterator) Iterator {
	compressIter, ok := it.(*compressIterator)
	if !ok {
		compressIter = &compressIterator{}
	}

	compressIter.Reset(c.b.bytes())
	return compressIter
}

type compressAppender struct {
	b *bstream
	w *lz4.Writer

	encBuf []byte
}

func (a *compressAppender) Append(t int64, v []byte) {
	num := binary.BigEndian.Uint32(a.b.bytes())

	if a.w == nil {
		a.w = getCompressWriter(a.b)
	}
	a.w.Write(a.encBuf[:binary.PutVarint(a.encBuf, t)])

	a.w.Write(a.encBuf[:binary.PutUvarint(a.encBuf, uint64(len(v)))])
	a.w.Write(v)

	binary.BigEndian.PutUint32(a.b.bytes(), num+1)
}

func (a *compressAppender) Close() (err error) {
	if a.w != nil {
		err = a.w.Close()
		putCompressWriter(a.w)

		if err != nil {
			//
		}
	}
	return
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
