package logql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	backendmsg "github.com/baudb/baudb/msg/backend"
)

func Test_logSelectorExpr_String(t *testing.T) {
	t.Parallel()
	tests := []string{
		`{foo!~"bar"}`,
		`{foo="bar", bar!="baz"}`,
		`{foo="bar", bar!="baz"} != "bip" !~ ".+bop"`,
		`{foo="bar"} |= "baz" |~ "blip" != "flip" !~ "flap"`,
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt, func(t *testing.T) {
			t.Parallel()
			expr, err := ParseLogSelector(tt)
			if err != nil {
				t.Fatalf("failed to parse log selector: %s", err)
			}
			if expr.String() != strings.Replace(tt, " ", "", -1) {
				t.Fatalf("error expected: %s got: %s", tt, expr.String())
			}
		})
	}
}

type linecheck struct {
	l string
	e bool
}

func Test_FilterMatcher(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		q string

		expectedMatchers []*backendmsg.Matcher
		// test line against the resulting filter, if empty filter should also be nil
		lines []linecheck
	}{
		{
			`{app="foo",cluster=~".+bar"}`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchEqual, "app", "foo"),
				mustNewMatcher(backendmsg.MatchRegexp, "cluster", ".+bar"),
			},
			nil,
		},
		{
			`{app!="foo",cluster=~".+bar",bar!~".?boo"}`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchNotEqual, "app", "foo"),
				mustNewMatcher(backendmsg.MatchRegexp, "cluster", ".+bar"),
				mustNewMatcher(backendmsg.MatchNotRegexp, "bar", ".?boo"),
			},
			nil,
		},
		{
			`{app="foo"} |= "foo"`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foobar", true}, {"bar", false}},
		},
		{
			`{app="foo"} |= "foo" != "bar"`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foobuzz", true}, {"bar", false}},
		},
		{
			`{app="foo"} |= "foo" !~ "f.*b"`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foo", true}, {"bar", false}, {"foobar", false}},
		},
		{
			`{app="foo"} |= "foo" |~ "f.*b"`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchEqual, "app", "foo"),
			},
			[]linecheck{{"foo", false}, {"bar", false}, {"foobar", true}},
		},
	} {
		tt := tt
		t.Run(tt.q, func(t *testing.T) {
			t.Parallel()
			expr, err := ParseLogSelector(tt.q)
			assert.Nil(t, err)
			assert.Equal(t, tt.expectedMatchers, expr.Matchers())
			filters := expr.Filters()
			assert.Nil(t, err)
			if tt.lines == nil {
				assert.Nil(t, filters)
			} else {
				f := func(line []byte) bool {
					for _, filter := range filters {
						fn, err := filter.FilterFn()
						assert.Nil(t, err)
						if !fn(line) {
							return false
						}
					}
					return true
				}

				for _, lc := range tt.lines {
					assert.Equal(t, lc.e, f([]byte(lc.l)))
				}
			}
		})
	}
}
