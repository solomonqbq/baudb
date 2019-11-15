package chunkenc

import (
	"io"
)

const preFetchSize = 256 << 10

// bstream is a stream of bits.
type bstream struct {
	stream []byte // the data stream
}

func (b *bstream) bytes() []byte {
	return b.stream
}

func (b *bstream) Write(p []byte) (n int, err error) {
	b.stream = append(b.stream, p...)
	return len(p), nil
}

type bReader struct {
	b    []byte
	i    int // current reading index
	left int
}

func newBReader(b []byte) *bReader {
	return &bReader{b, 0, len(b)}
}

func (r *bReader) Read(b []byte) (n int, err error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n = copy(b, r.b[r.i:])
	r.i += n
	r.left -= n

	//if r.left >= preFetchSize {
	//	fileutil.MAdviseWillNeed(r.b[r.i : r.i+preFetchSize])
	//} else {
	//	fileutil.MAdviseWillNeed(r.b[r.i:])
	//}
	return
}
