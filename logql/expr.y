%{
package logql

import (
  backendmsg "github.com/baudb/baudb/msg/backend"
)
%}

%union{
  Expr                    Expr
  Filter                  backendmsg.MatchType
  Labels                  []string
  LogExpr                 LogSelectorExpr
  Matcher                 *backendmsg.Matcher
  Matchers                []*backendmsg.Matcher
  Selector                []*backendmsg.Matcher
  str                     string
  int                     int64
}

%start root

%type <Expr>                  expr
%type <Filter>                filter
%type <Labels>                labels
%type <LogExpr>               logExpr
%type <Matcher>               matcher
%type <Matchers>              matchers
%type <Selector>              selector

%token <str>      IDENTIFIER STRING
%token <val>      MATCHERS LABELS EQ NEQ RE NRE OPEN_BRACE CLOSE_BRACE COMMA DOT PIPE_MATCH PIPE_EXACT
                  OPEN_PARENTHESIS CLOSE_PARENTHESIS

%%

root: expr { exprlex.(*lexer).expr = $1 };

expr:
      logExpr                    { $$ = $1 }
    ;

logExpr:
      selector                                    { $$ = newMatcherExpr($1)}
    | logExpr filter STRING                       { $$ = NewFilterExpr( $1, $2, $3 ) }
    | OPEN_PARENTHESIS logExpr CLOSE_PARENTHESIS  { $$ = $2}
    | logExpr filter error
    | logExpr error
    ;

filter:
      PIPE_MATCH                       { $$ = backendmsg.MatchRegexp }
    | PIPE_EXACT                       { $$ = backendmsg.MatchEqual }
    | NRE                              { $$ = backendmsg.MatchNotRegexp }
    | NEQ                              { $$ = backendmsg.MatchNotEqual }
    ;

selector:
      OPEN_BRACE matchers CLOSE_BRACE  { $$ = $2 }
    | OPEN_BRACE matchers error        { $$ = $2 }
    | OPEN_BRACE error CLOSE_BRACE     { }
    ;

matchers:
      matcher                          { $$ = []*backendmsg.Matcher{ $1 } }
    | matchers COMMA matcher           { $$ = append($1, $3) }
    ;

matcher:
      IDENTIFIER EQ STRING             { $$ = mustNewMatcher(backendmsg.MatchEqual, $1, $3) }
    | IDENTIFIER NEQ STRING            { $$ = mustNewMatcher(backendmsg.MatchNotEqual, $1, $3) }
    | IDENTIFIER RE STRING             { $$ = mustNewMatcher(backendmsg.MatchRegexp, $1, $3) }
    | IDENTIFIER NRE STRING            { $$ = mustNewMatcher(backendmsg.MatchNotRegexp, $1, $3) }
    ;

labels:
      IDENTIFIER                 { $$ = []string{ $1 } }
    | labels COMMA IDENTIFIER    { $$ = append($1, $3) }
    ;
%%
