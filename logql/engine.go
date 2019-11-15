package logql

import (
	"context"
	"time"

	"github.com/baudb/baudb/backend"
	"github.com/baudb/baudb/msg"
	ts "github.com/baudb/baudb/util/time"
)

// Value is a generic interface for values resulting from a query evaluation.
type Value interface {
	//msg.Message
	Type() ValueType
}

// ValueType describes a type of a value.
type ValueType string

// ValueTypeStreams promql.ValueType for log streams
const ValueTypeStreams = "logStreams"

// LogStreams is promql.Value
type LogStreams []*msg.Series

// Type implements `promql.Value`
func (LogStreams) Type() ValueType { return ValueTypeStreams }

// Engine is the LogQL engine.
type Engine struct {
	timeout time.Duration
}

// NewEngine creates a new LogQL engine.
func NewEngine(timeout time.Duration) *Engine {
	return &Engine{
		timeout: timeout,
	}
}

// Query is a LogQL query to be executed.
type Query interface {
	// Exec processes the query.
	Exec(ctx context.Context) (Value, error)
}

type query struct {
	queryable  backend.Queryable
	qs         string
	start, end time.Time
	offset     int
	limit      int

	ng *Engine
}

func (q *query) Exec(ctx context.Context) (Value, error) {
	return q.ng.exec(ctx, q)
}

func (ng *Engine) NewQuery(q backend.Queryable, qs string, start, end time.Time, offset int, limit int) Query {
	return &query{
		queryable: q,
		qs:        qs,
		start:     start,
		end:       end,
		offset:    offset,
		limit:     limit,
		ng:        ng,
	}
}

func (ng *Engine) exec(ctx context.Context, q *query) (Value, error) {
	ctx, cancel := context.WithTimeout(ctx, ng.timeout)
	defer cancel()

	expr, err := ParseExpr(q.qs)
	if err != nil {
		return nil, err
	}

	switch e := expr.(type) {
	case LogSelectorExpr:
		mint := ts.FromTime(q.start)
		maxt := ts.FromTime(q.end)

		querier, err := q.queryable.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}

		params := &backend.SelectParams{
			Offset:  q.offset,
			Limit:   q.limit,
			Filters: e.Filters(),
		}
		matchers := e.Matchers()

		set, err := querier.Select(params, matchers...)
		if err != nil {
			return nil, err
		}

		return readStreams(ctx, set, q.limit)
	}

	return nil, nil
}

func readStreams(ctx context.Context, set backend.SeriesSet, limit int) (res LogStreams, err error) {
	var (
		series     []*msg.Series
		points     []msg.Point
		it         backend.SeriesIterator
	)

	for set.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		curSeries := set.At()

		start := len(points)

		it = curSeries.Iterator()

		for (limit < 0 || len(points) < limit) && it.Next() {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			t, v := it.At()

			points = append(points, msg.Point{T: t, V: v})
		}

		if len(points[start:]) > 0 {
			s := &msg.Series{
				Labels: curSeries.Labels(),
				Points: points[start:],
			}
			series = append(series, s)
		}
	}

	return series, nil
}
