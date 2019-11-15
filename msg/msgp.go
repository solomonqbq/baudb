//go:generate msgp -io=false -tests=false

package msg

import (
	"encoding/json"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/tinylib/msgp/msgp"
)

const (
	MetricName = "__name__"
	sep        = '\xff'
)

type StatusCode byte

const (
	StatusCode_Succeed StatusCode = iota
	StatusCode_Failed
)

//msgp:tuple Label
type Label struct {
	Name  string `msg:"name"`
	Value string `msg:"value"`
}

func (ls Labels) Hash() uint64 {
	b := make([]byte, 0, 1024)

	for _, v := range ls {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

type Labels []Label

func (ls Labels) Len() int           { return len(ls) }
func (ls Labels) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls Labels) Less(i, j int) bool { return ls[i].Name < ls[j].Name }

//msgp:tuple Point
type Point struct {
	T int64  `msg:"T"`
	V []byte `msg:"V"`
}

func (p Point) MarshalJSON() ([]byte, error) {
	return json.Marshal([...]interface{}{p.T, *(*string)(unsafe.Pointer(&p.V))})
}

//msgp:ignore Points
type Points []Point

func (ps Points) Len() int           { return len(ps) }
func (ps Points) Swap(i, j int)      { ps[i], ps[j] = ps[j], ps[i] }
func (ps Points) Less(i, j int) bool { return ps[i].T < ps[j].T }

func (ps Points) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, ps.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(ps)))
	for i := range ps {
		// array header, size 2
		o = append(o, 0x92)
		o = msgp.AppendInt64(o, ps[i].T)
		o = msgp.AppendBytes(o, ps[i].V)
	}
	return
}

func (ps *Points) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var l uint32
	l, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*ps)) >= int(l) {
		(*ps) = (*ps)[:l]
	} else {
		(*ps) = make(Points, l)
	}
	for i := range *ps {
		var header uint32
		header, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, i)
			return
		}
		if header != 2 {
			err = msgp.ArrayError{Wanted: 2, Got: header}
			return
		}
		(*ps)[i].T, bts, err = msgp.ReadInt64Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, i, "T")
			return
		}
		(*ps)[i].V, bts, err = msgp.ReadBytesBytes(bts, nil) //ReadBytesBytes(bts, (*ps)[i].V)
		if err != nil {
			err = msgp.WrapError(err, i, "V")
			return
		}
	}
	o = bts
	return
}

func (ps Points) Msgsize() (sz int) {
	sz = msgp.ArrayHeaderSize
	for i := range ps {
		sz += 1 + msgp.Int64Size + msgp.BytesPrefixSize + len(ps[i].V)
	}
	return
}

//msgp:tuple Series
type Series struct {
	Labels Labels `msg:"labels"`
	Points Points `msg:"points"`
}

type LabelValuesResponse struct {
	Values   []string   `msg:"values"`
	Status   StatusCode `msg:"status"`
	ErrorMsg string     `msg:"errorMsg"`
}

type GeneralResponse struct {
	Status  StatusCode `msg:"status"`
	Message string     `msg:"message"`
}

//msgp:tuple MultiResponse
type MultiResponse struct {
	Resp []Message `msg:"resp"`
}

func (multi *MultiResponse) Append(resp Message) *MultiResponse {
	multi.Resp = append(multi.Resp, resp)
	return multi
}

func Equal(ls, o Labels) bool {
	if len(ls) != len(o) {
		return false
	}
	for i, l := range ls {
		if l.Name != o[i].Name || l.Value != o[i].Value {
			return false
		}
	}
	return true
}
