package logql

import (
	"fmt"
	"strings"

	backendmsg "github.com/baudb/baudb/msg/backend"
)

// Expr is the root expression which can be a SampleExpr or LogSelectorExpr
type Expr interface{}

// LogSelectorExpr is a LogQL expression filtering and returning logs.
type LogSelectorExpr interface {
	Filters() []*backendmsg.Filter
	Matchers() []*backendmsg.Matcher
	fmt.Stringer
}

type matchersExpr struct {
	matchers []*backendmsg.Matcher
}

func newMatcherExpr(matchers []*backendmsg.Matcher) LogSelectorExpr {
	return &matchersExpr{matchers: matchers}
}

func (e *matchersExpr) Matchers() []*backendmsg.Matcher {
	return e.matchers
}

func (e *matchersExpr) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	for i, m := range e.matchers {
		sb.WriteString(m.String())
		if i+1 != len(e.matchers) {
			sb.WriteString(",")
		}
	}
	sb.WriteString("}")
	return sb.String()
}

func (e *matchersExpr) Filters() []*backendmsg.Filter {
	return nil
}

type filterExpr struct {
	left  LogSelectorExpr
	ty    backendmsg.MatchType
	match string
}

// NewFilterExpr wraps an existing Expr with a next filter expression.
func NewFilterExpr(left LogSelectorExpr, ty backendmsg.MatchType, match string) LogSelectorExpr {
	return &filterExpr{
		left:  left,
		ty:    ty,
		match: match,
	}
}

func (e *filterExpr) Matchers() []*backendmsg.Matcher {
	return e.left.Matchers()
}

func (e *filterExpr) String() string {
	var sb strings.Builder
	sb.WriteString(e.left.String())
	switch e.ty {
	case backendmsg.MatchRegexp:
		sb.WriteString("|~")
	case backendmsg.MatchNotRegexp:
		sb.WriteString("!~")
	case backendmsg.MatchEqual:
		sb.WriteString("|=")
	case backendmsg.MatchNotEqual:
		sb.WriteString("!=")
	}
	sb.WriteString(e.match)
	return sb.String()
}

func (e *filterExpr) Filters() []*backendmsg.Filter {
	var filters []*backendmsg.Filter

	if leftFilterExpr, ok := e.left.(*filterExpr); ok {
		filters = leftFilterExpr.Filters()
	}

	filters = append(filters, &backendmsg.Filter{
		Type:  e.ty,
		Value: e.match,
	})
	return filters
}

func mustNewMatcher(t backendmsg.MatchType, n, v string) *backendmsg.Matcher {
	return &backendmsg.Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
}
