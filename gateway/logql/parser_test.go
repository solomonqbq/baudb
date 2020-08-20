package logql

import (
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		in  string
		exp Expr
		err error
	}{
		{
			in:  `{foo="bar"}`,
			exp: &matchersExpr{matchers: []*backendmsg.Matcher{mustNewMatcher(backendmsg.MatchEqual, "foo", "bar")}},
		},
		{
			in:  `{ foo = "bar" }`,
			exp: &matchersExpr{matchers: []*backendmsg.Matcher{mustNewMatcher(backendmsg.MatchEqual, "foo", "bar")}},
		},
		{
			in:  `{ foo != "bar" }`,
			exp: &matchersExpr{matchers: []*backendmsg.Matcher{mustNewMatcher(backendmsg.MatchNotEqual, "foo", "bar")}},
		},
		{
			in:  `{ foo =~ "bar" }`,
			exp: &matchersExpr{matchers: []*backendmsg.Matcher{mustNewMatcher(backendmsg.MatchRegexp, "foo", "bar")}},
		},
		{
			in:  `{ foo !~ "bar" }`,
			exp: &matchersExpr{matchers: []*backendmsg.Matcher{mustNewMatcher(backendmsg.MatchNotRegexp, "foo", "bar")}},
		},
		{
			in: `unk({ foo !~ "bar" }[5m])`,
			err: ParseError{
				msg:  "syntax error: unexpected IDENTIFIER",
				line: 1,
				col:  1,
			},
		},
		{
			in: `rate({ foo !~ "bar" }[5minutes])`,
			err: ParseError{
				msg:  "time: unknown unit minutes in duration 5minutes",
				line: 0,
				col:  22,
			},
		},
		{
			in: `rate({ foo !~ "bar" }[5)`,
			err: ParseError{
				msg:  "missing closing ']' in duration",
				line: 0,
				col:  22,
			},
		},
		{
			in: `min({ foo !~ "bar" }[5m])`,
			err: ParseError{
				msg:  "syntax error: unexpected {",
				line: 1,
				col:  5,
			},
		},
		{
			in: `sum(3 ,count_over_time({ foo !~ "bar" }[5h]))`,
			err: ParseError{
				msg:  "unsupported parameter for operation sum(3,",
				line: 0,
				col:  0,
			},
		},
		{
			in: `topk(count_over_time({ foo !~ "bar" }[5h]))`,
			err: ParseError{
				msg:  "parameter required for operation topk",
				line: 0,
				col:  0,
			},
		},
		{
			in: `bottomk(he,count_over_time({ foo !~ "bar" }[5h]))`,
			err: ParseError{
				msg:  "invalid parameter bottomk(he,",
				line: 0,
				col:  0,
			},
		},
		{
			in: `stddev({ foo !~ "bar" })`,
			err: ParseError{
				msg:  "syntax error: unexpected {",
				line: 1,
				col:  8,
			},
		},
		{
			in: `{ foo = "bar", bar != "baz" }`,
			exp: &matchersExpr{matchers: []*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchEqual, "foo", "bar"),
				mustNewMatcher(backendmsg.MatchNotEqual, "bar", "baz"),
			}},
		},
		{
			in: `{foo="bar"} |= "baz"`,
			exp: &filterExpr{
				left:  &matchersExpr{matchers: []*backendmsg.Matcher{mustNewMatcher(backendmsg.MatchEqual, "foo", "bar")}},
				ty:    backendmsg.MatchEqual,
				match: "baz",
			},
		},
		{
			in: `{foo="bar"} |= "baz" |~ "blip" != "flip" !~ "flap"`,
			exp: &filterExpr{
				left: &filterExpr{
					left: &filterExpr{
						left: &filterExpr{
							left:  &matchersExpr{matchers: []*backendmsg.Matcher{mustNewMatcher(backendmsg.MatchEqual, "foo", "bar")}},
							ty:    backendmsg.MatchEqual,
							match: "baz",
						},
						ty:    backendmsg.MatchRegexp,
						match: "blip",
					},
					ty:    backendmsg.MatchNotEqual,
					match: "flip",
				},
				ty:    backendmsg.MatchNotRegexp,
				match: "flap",
			},
		},
		{
			in: `{foo="bar}`,
			err: ParseError{
				msg:  "literal not terminated",
				line: 1,
				col:  6,
			},
		},
		{
			in: `{foo="bar"`,
			err: ParseError{
				msg:  "syntax error: unexpected $end, expecting } or ,",
				line: 1,
				col:  11,
			},
		},

		{
			in: `{foo="bar"} |~`,
			err: ParseError{
				msg:  "syntax error: unexpected $end, expecting STRING",
				line: 1,
				col:  15,
			},
		},

		{
			in: `{foo="bar"} "foo"`,
			err: ParseError{
				msg:  "syntax error: unexpected STRING, expecting != or !~ or |~ or |=",
				line: 1,
				col:  13,
			},
		},
		{
			in: `{foo="bar"} foo`,
			err: ParseError{
				msg:  "syntax error: unexpected IDENTIFIER, expecting != or !~ or |~ or |=",
				line: 1,
				col:  13,
			},
		},
	} {
		t.Run(tc.in, func(t *testing.T) {
			ast, err := ParseExpr(tc.in)
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.exp, ast)
		})
	}
}

func TestParseMatchers(t *testing.T) {

	tests := []struct {
		input   string
		want    []*backendmsg.Matcher
		wantErr bool
	}{
		{
			`{app="foo",cluster=~".+bar"}`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchEqual, "app", "foo"),
				mustNewMatcher(backendmsg.MatchRegexp, "cluster", ".+bar"),
			},
			false,
		},
		{
			`{app!="foo",cluster=~".+bar",bar!~".?boo"}`,
			[]*backendmsg.Matcher{
				mustNewMatcher(backendmsg.MatchNotEqual, "app", "foo"),
				mustNewMatcher(backendmsg.MatchRegexp, "cluster", ".+bar"),
				mustNewMatcher(backendmsg.MatchNotRegexp, "bar", ".?boo"),
			},
			false,
		},
		{
			`{app!="foo",cluster=~".+bar",bar!~".?boo"`,
			nil,
			true,
		},
		{
			`{app!="foo",cluster=~".+bar",bar!~".?boo"} |= "test"`,
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseMatchers(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMatchers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseMatchers() = %v, want %v", got, tt.want)
			}
		})
	}
}
