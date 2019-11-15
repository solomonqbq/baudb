//go:generate msgp -io=false -tests=false

package backend

import (
	"bytes"
	"fmt"
	"github.com/baudb/baudb/msg"
	"github.com/baudb/baudb/util"
	"regexp"
	"strings"
)

type MatchType byte

const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (z MatchType) String() string {
	switch z {
	case MatchEqual:
		return "Equal"
	case MatchNotEqual:
		return "NotEqual"
	case MatchRegexp:
		return "Regexp"
	case MatchNotRegexp:
		return "NotRegexp"
	}
	return "<Invalid>"
}

type Matcher struct {
	Type  MatchType `msg:"Type"`
	Name  string    `msg:"Name"`
	Value string    `msg:"Value"`
}

func (m *Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

type Filter struct {
	Type  MatchType
	Value string
}

func (f *Filter) FilterFn() (func(v interface{}) bool, error) {
	var fn func(interface{}) bool
	switch f.Type {
	case MatchRegexp:
		re, err := regexp.Compile(f.Value)
		if err != nil {
			return nil, err
		}
		fn = func(v interface{}) bool {
			if data, ok := v.([]byte); ok {
				return re.Match(data)
			}
			if data, ok := v.(string); ok {
				return re.MatchString(data)
			}
			return false
		}
	case MatchNotRegexp:
		re, err := regexp.Compile(f.Value)
		if err != nil {
			return nil, err
		}
		fn = func(v interface{}) bool {
			if data, ok := v.([]byte); ok {
				return !re.Match(data)
			}
			if data, ok := v.(string); ok {
				return !re.MatchString(data)
			}
			return true
		}
	case MatchEqual:
		fn = func(v interface{}) bool {
			if data, ok := v.([]byte); ok {
				return bytes.Contains(data, util.YoloBytes(f.Value))
			}
			if data, ok := v.(string); ok {
				return strings.Contains(data, f.Value)
			}
			return false
		}
	case MatchNotEqual:
		fn = func(v interface{}) bool {
			if data, ok := v.([]byte); ok {
				return !bytes.Contains(data, util.YoloBytes(f.Value))
			}
			if data, ok := v.(string); ok {
				return !strings.Contains(data, f.Value)
			}
			return true
		}
	default:
		return nil, fmt.Errorf("unknown matcher: %v", f.Value)
	}
	return fn, nil
}

type SelectRequest struct {
	Mint       int64      `msg:"mint"`
	Maxt       int64      `msg:"maxt"`
	Matchers   []*Matcher `msg:"matchers"`
	Filters    []*Filter  `msg:"filters"`
	Offset     int        `msg:"offset"`
	Limit      int        `msg:"limit"`
	OnlyLabels bool       `msg:"onlyLB"`
	SpanCtx    []byte     `msg:"spanCtx"`
}

type SelectResponse struct {
	Status   msg.StatusCode `msg:"status"`
	Series   []*msg.Series  `msg:"series"`
	ErrorMsg string         `msg:"errorMsg"`
}

//msgp:tuple AddRequest
type AddRequest struct {
	Series []*msg.Series `msg:"series"`
}

type LabelValuesRequest struct {
	Mint     int64      `msg:"mint"`
	Maxt     int64      `msg:"maxt"`
	Name     string     `msg:"name"`
	Matchers []*Matcher `msg:"matchers"`
	SpanCtx  []byte     `msg:"spanCtx"`
}
