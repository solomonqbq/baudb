package logql

import (
	"strconv"
	"text/scanner"
)

var tokens = map[string]int{
	",":                 COMMA,
	".":                 DOT,
	"{":                 OPEN_BRACE,
	"}":                 CLOSE_BRACE,
	"=":                 EQ,
	"!=":                NEQ,
	"=~":                RE,
	"!~":                NRE,
	"|=":                PIPE_EXACT,
	"|~":                PIPE_MATCH,
	"(":                 OPEN_PARENTHESIS,
	")":                 CLOSE_PARENTHESIS,
}

type lexer struct {
	scanner.Scanner
	errs   []ParseError
	expr   Expr
	parser *exprParserImpl
}

func (l *lexer) Lex(lval *exprSymType) int {
	r := l.Scan()
	switch r {
	case scanner.EOF:
		return 0

	case scanner.String:
		var err error
		lval.str, err = strconv.Unquote(l.TokenText())
		if err != nil {
			l.Error(err.Error())
			return 0
		}
		return STRING
	}

	if tok, ok := tokens[l.TokenText()+string(l.Peek())]; ok {
		l.Next()
		return tok
	}

	if tok, ok := tokens[l.TokenText()]; ok {
		return tok
	}

	lval.str = l.TokenText()
	return IDENTIFIER
}

func (l *lexer) Error(msg string) {
	l.errs = append(l.errs, newParseError(msg, l.Line, l.Column))
}
