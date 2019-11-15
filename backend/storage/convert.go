package storage

import (
	"github.com/baudb/baudb/backend/storage/labels"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"strings"
)

func ProtoToMatchers(matchers []*backendmsg.Matcher) ([]labels.Matcher, error) {
	result := make([]labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		result = append(result, ProtoToMatcher(m))
	}
	return result, nil
}

func ProtoToMatcher(m *backendmsg.Matcher) labels.Matcher {
	switch m.Type {
	case backendmsg.MatchEqual:
		return labels.NewEqualMatcher(m.Name, m.Value)

	case backendmsg.MatchNotEqual:
		return labels.Not(labels.NewEqualMatcher(m.Name, m.Value))

	case backendmsg.MatchRegexp:
		res, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return res

	case backendmsg.MatchNotRegexp:
		res, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return labels.Not(res)
	}
	panic("storage.convertMatcher: invalid matcher type")
}

func LabelsToProto(lbs labels.Labels) []msg.Label {
	proto := make([]msg.Label, 0, len(lbs))
	for _, l := range lbs {
		proto = append(proto, msg.Label{Name: l.Name, Value: l.Value})
	}
	return proto
}

func toString(lbs []msg.Label) string {
	var b strings.Builder

	b.WriteByte('{')
	for i, l := range lbs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
	}
	b.WriteByte('}')

	return b.String()
}
