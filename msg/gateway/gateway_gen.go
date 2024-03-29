package gateway

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/baudb/baudb/msg"
	"github.com/tinylib/msgp/msgp"
)

// MarshalMsg implements msgp.Marshaler
func (z *AddRequest) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// array header, size 1
	o = append(o, 0x91)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Series)))
	for za0001 := range z.Series {
		if z.Series[za0001] == nil {
			o = msgp.AppendNil(o)
		} else {
			o, err = z.Series[za0001].MarshalMsg(o)
			if err != nil {
				err = msgp.WrapError(err, "Series", za0001)
				return
			}
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *AddRequest) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 1 {
		err = msgp.ArrayError{Wanted: 1, Got: zb0001}
		return
	}
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Series")
		return
	}
	if cap(z.Series) >= int(zb0002) {
		z.Series = (z.Series)[:zb0002]
	} else {
		z.Series = make([]*msg.Series, zb0002)
	}
	for za0001 := range z.Series {
		if msgp.IsNil(bts) {
			bts, err = msgp.ReadNilBytes(bts)
			if err != nil {
				return
			}
			z.Series[za0001] = nil
		} else {
			if z.Series[za0001] == nil {
				z.Series[za0001] = new(msg.Series)
			}
			bts, err = z.Series[za0001].UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "Series", za0001)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *AddRequest) Msgsize() (s int) {
	s = 1 + msgp.ArrayHeaderSize
	for za0001 := range z.Series {
		if z.Series[za0001] == nil {
			s += msgp.NilSize
		} else {
			s += z.Series[za0001].Msgsize()
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z LabelValuesRequest) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "name"
	o = append(o, 0x83, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "constraint"
	o = append(o, 0xaa, 0x63, 0x6f, 0x6e, 0x73, 0x74, 0x72, 0x61, 0x69, 0x6e, 0x74)
	o = msgp.AppendString(o, z.Constraint)
	// string "timeout"
	o = append(o, 0xa7, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74)
	o = msgp.AppendString(o, z.Timeout)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *LabelValuesRequest) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Name")
				return
			}
		case "constraint":
			z.Constraint, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Constraint")
				return
			}
		case "timeout":
			z.Timeout, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Timeout")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z LabelValuesRequest) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Name) + 11 + msgp.StringPrefixSize + len(z.Constraint) + 8 + msgp.StringPrefixSize + len(z.Timeout)
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *SelectRequest) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "query"
	o = append(o, 0x86, 0xa5, 0x71, 0x75, 0x65, 0x72, 0x79)
	o = msgp.AppendString(o, z.Query)
	// string "start"
	o = append(o, 0xa5, 0x73, 0x74, 0x61, 0x72, 0x74)
	o = msgp.AppendString(o, z.Start)
	// string "end"
	o = append(o, 0xa3, 0x65, 0x6e, 0x64)
	o = msgp.AppendString(o, z.End)
	// string "offset"
	o = append(o, 0xa6, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74)
	o = msgp.AppendInt(o, z.Offset)
	// string "limit"
	o = append(o, 0xa5, 0x6c, 0x69, 0x6d, 0x69, 0x74)
	o = msgp.AppendInt(o, z.Limit)
	// string "timeout"
	o = append(o, 0xa7, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74)
	o = msgp.AppendString(o, z.Timeout)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SelectRequest) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "query":
			z.Query, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Query")
				return
			}
		case "start":
			z.Start, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Start")
				return
			}
		case "end":
			z.End, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "End")
				return
			}
		case "offset":
			z.Offset, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Offset")
				return
			}
		case "limit":
			z.Limit, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Limit")
				return
			}
		case "timeout":
			z.Timeout, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Timeout")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *SelectRequest) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Query) + 6 + msgp.StringPrefixSize + len(z.Start) + 4 + msgp.StringPrefixSize + len(z.End) + 7 + msgp.IntSize + 6 + msgp.IntSize + 8 + msgp.StringPrefixSize + len(z.Timeout)
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *SelectResponse) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "result"
	o = append(o, 0x83, 0xa6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74)
	o = msgp.AppendString(o, z.Result)
	// string "status"
	o = append(o, 0xa6, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73)
	o, err = z.Status.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "Status")
		return
	}
	// string "errorMsg"
	o = append(o, 0xa8, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x4d, 0x73, 0x67)
	o = msgp.AppendString(o, z.ErrorMsg)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SelectResponse) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "result":
			z.Result, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Result")
				return
			}
		case "status":
			bts, err = z.Status.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "Status")
				return
			}
		case "errorMsg":
			z.ErrorMsg, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ErrorMsg")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *SelectResponse) Msgsize() (s int) {
	s = 1 + 7 + msgp.StringPrefixSize + len(z.Result) + 7 + z.Status.Msgsize() + 9 + msgp.StringPrefixSize + len(z.ErrorMsg)
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *SeriesLabelsRequest) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "matches"
	o = append(o, 0x84, 0xa7, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Matches)))
	for za0001 := range z.Matches {
		o = msgp.AppendString(o, z.Matches[za0001])
	}
	// string "start"
	o = append(o, 0xa5, 0x73, 0x74, 0x61, 0x72, 0x74)
	o = msgp.AppendString(o, z.Start)
	// string "end"
	o = append(o, 0xa3, 0x65, 0x6e, 0x64)
	o = msgp.AppendString(o, z.End)
	// string "timeout"
	o = append(o, 0xa7, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74)
	o = msgp.AppendString(o, z.Timeout)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesLabelsRequest) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "matches":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Matches")
				return
			}
			if cap(z.Matches) >= int(zb0002) {
				z.Matches = (z.Matches)[:zb0002]
			} else {
				z.Matches = make([]string, zb0002)
			}
			for za0001 := range z.Matches {
				z.Matches[za0001], bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Matches", za0001)
					return
				}
			}
		case "start":
			z.Start, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Start")
				return
			}
		case "end":
			z.End, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "End")
				return
			}
		case "timeout":
			z.Timeout, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Timeout")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *SeriesLabelsRequest) Msgsize() (s int) {
	s = 1 + 8 + msgp.ArrayHeaderSize
	for za0001 := range z.Matches {
		s += msgp.StringPrefixSize + len(z.Matches[za0001])
	}
	s += 6 + msgp.StringPrefixSize + len(z.Start) + 4 + msgp.StringPrefixSize + len(z.End) + 8 + msgp.StringPrefixSize + len(z.Timeout)
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *SeriesLabelsResponse) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "labels"
	o = append(o, 0x83, 0xa6, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Labels)))
	for za0001 := range z.Labels {
		o = msgp.AppendArrayHeader(o, uint32(len(z.Labels[za0001])))
		for za0002 := range z.Labels[za0001] {
			o, err = z.Labels[za0001][za0002].MarshalMsg(o)
			if err != nil {
				err = msgp.WrapError(err, "Labels", za0001, za0002)
				return
			}
		}
	}
	// string "status"
	o = append(o, 0xa6, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73)
	o, err = z.Status.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "Status")
		return
	}
	// string "errorMsg"
	o = append(o, 0xa8, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x4d, 0x73, 0x67)
	o = msgp.AppendString(o, z.ErrorMsg)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesLabelsResponse) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "labels":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Labels")
				return
			}
			if cap(z.Labels) >= int(zb0002) {
				z.Labels = (z.Labels)[:zb0002]
			} else {
				z.Labels = make([][]msg.Label, zb0002)
			}
			for za0001 := range z.Labels {
				var zb0003 uint32
				zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Labels", za0001)
					return
				}
				if cap(z.Labels[za0001]) >= int(zb0003) {
					z.Labels[za0001] = (z.Labels[za0001])[:zb0003]
				} else {
					z.Labels[za0001] = make([]msg.Label, zb0003)
				}
				for za0002 := range z.Labels[za0001] {
					bts, err = z.Labels[za0001][za0002].UnmarshalMsg(bts)
					if err != nil {
						err = msgp.WrapError(err, "Labels", za0001, za0002)
						return
					}
				}
			}
		case "status":
			bts, err = z.Status.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "Status")
				return
			}
		case "errorMsg":
			z.ErrorMsg, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ErrorMsg")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *SeriesLabelsResponse) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for za0001 := range z.Labels {
		s += msgp.ArrayHeaderSize
		for za0002 := range z.Labels[za0001] {
			s += z.Labels[za0001][za0002].Msgsize()
		}
	}
	s += 7 + z.Status.Msgsize() + 9 + msgp.StringPrefixSize + len(z.ErrorMsg)
	return
}
