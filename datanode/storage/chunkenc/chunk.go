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

package chunkenc

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "XOR"
	case EncCompress:
		return "compress"
	}
	return "<unknown>"
}

// The different available chunk encodings.
const (
	EncNone Encoding = iota
	EncXOR
	EncCompress
)

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
type Chunk interface {
	Bytes() []byte
	Encoding() Encoding
	Appender() (Appender, error)
	// The iterator passed as argument is for re-use.
	// Depending on implementation, the iterator can
	// be re-used or a new iterator can be allocated.
	Iterator(Iterator) Iterator
	NumSamples() int
}

// Appender adds sample pairs to a chunk.
type Appender interface {
	Append(int64, []byte)
}

type AppendCloser interface {
	Append(int64, []byte)
	io.Closer
}

// Iterator is a simple iterator that can only get the next value.
type Iterator interface {
	At() (int64, []byte)
	Err() error
	Next() bool
}

// NewNopIterator returns a new chunk iterator that does not hold any data.
func NewNopIterator() Iterator {
	return nopIterator{}
}

type nopIterator struct{}

func (nopIterator) At() (int64, []byte) { return 0, nil }
func (nopIterator) Next() bool          { return false }
func (nopIterator) Err() error          { return nil }

// Pool is used to create and reuse chunk references to avoid allocations.
type Pool interface {
	Put(Chunk) error
	Get(e Encoding, b []byte) (Chunk, error)
}

// pool is a memory pool of chunk objects.
type pool struct {
	cpool sync.Pool
}

// NewPool returns a new pool.
func NewPool() Pool {
	return &pool{
		cpool: sync.Pool{
			New: func() interface{} {
				return &CompressChunk{b: bstream{}}
			},
		},
	}
}

func (p *pool) Get(e Encoding, b []byte) (Chunk, error) {
	switch e {
	case EncCompress:
		c := p.cpool.Get().(*CompressChunk)

		c.b.stream = b
		return c, nil
	}
	return nil, errors.Errorf("invalid encoding %q", e)
}

func (p *pool) Put(c Chunk) error {
	switch c.Encoding() {
	case EncCompress:
		cc, ok := c.(*CompressChunk)
		// This may happen often with wrapped chunks. Nothing we can really do about
		// it but returning an error would cause a lot of allocations again. Thus,
		// we just skip it.
		if !ok {
			return nil
		}

		cc.b.stream = nil //cc.b.stream=cc.b.stream[:0]?

		p.cpool.Put(c)
	default:
		return errors.Errorf("invalid encoding %q", c.Encoding())
	}
	return nil
}
