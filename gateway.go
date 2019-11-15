package baudb

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/baudb/baudb/backend"
	"github.com/baudb/baudb/logql"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	gatewaymsg "github.com/baudb/baudb/msg/gateway"
	"github.com/baudb/baudb/util"
	ts "github.com/baudb/baudb/util/time"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/valyala/fasthttp"
	"go.uber.org/multierr"
)

type queryResult struct {
	ResultType logql.ValueType `json:"resultType"`
	Result     logql.Value     `json:"result"`
}

type Gateway struct {
	Backend      backend.Backend
	QueryEngine  *logql.Engine
	appenderPool sync.Pool
}

func (gateway *Gateway) SelectQuery(request *gatewaymsg.SelectRequest) *gatewaymsg.SelectResponse {
	result, err := gateway.selectQuery(request.Start, request.End, request.Timeout, request.Query, request.Offset, request.Limit)
	if err != nil {
		return &gatewaymsg.SelectResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}

	queryRes, err := json.Marshal(result)
	if err != nil {
		return &gatewaymsg.SelectResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}

	return &gatewaymsg.SelectResponse{Status: msg.StatusCode_Succeed, Result: string(queryRes)}
}

func (gateway *Gateway) SeriesLabels(request *gatewaymsg.SeriesLabelsRequest) *gatewaymsg.SeriesLabelsResponse {
	labels, err := gateway.seriesLabels(request.Matches, request.Start, request.End, request.Timeout)
	if err != nil {
		return &gatewaymsg.SeriesLabelsResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}
	return &gatewaymsg.SeriesLabelsResponse{Status: msg.StatusCode_Succeed, Labels: labels}
}

func (gateway *Gateway) LabelValues(request *gatewaymsg.LabelValuesRequest) *msg.LabelValuesResponse {
	values, err := gateway.labelValues(request.Name, request.Constraint, request.Timeout)
	if err != nil {
		return &msg.LabelValuesResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}
	return &msg.LabelValuesResponse{Status: msg.StatusCode_Succeed, Values: values}
}

func (gateway *Gateway) Ingest(request *gatewaymsg.AddRequest) error {
	var err error
	var appender backend.Appender

	if v := gateway.appenderPool.Get(); v != nil {
		appender = v.(backend.Appender)
	} else {
		appender, err = gateway.Backend.Appender()
		if err != nil {
			return errors.Errorf("no suitable appender: %v", err)
		}
	}

	var hasher = util.NewHasher()
	for _, series := range request.Series {
		hash := hasher.Hash(series.Labels)

		for _, p := range series.Points {
			if er := appender.Add(series.Labels, p.T, p.V, hash); er != nil {
				err = multierr.Append(err, er)
			}
		}
	}

	if er := appender.Flush(); er != nil {
		err = multierr.Append(err, er)
	}

	gateway.appenderPool.Put(appender)
	return err
}

type httpResponse struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
}

func (gateway *Gateway) HttpSelectQuery(c *fasthttp.RequestCtx) {
	exeHttpQuery(c, func() (interface{}, error) {
		var httpArgs *fasthttp.Args

		if util.YoloString(c.Method()) == "GET" {
			httpArgs = c.QueryArgs()
		} else {
			httpArgs = c.PostArgs()
		}

		var (
			start, end, timeout, query string
			offset, limit              int
			err                        error
		)

		if arg := httpArgs.Peek("start"); len(arg) > 0 {
			start = string(arg)
		}

		if arg := httpArgs.Peek("end"); len(arg) > 0 {
			end = string(arg)
		}

		if arg := httpArgs.Peek("timeout"); len(arg) > 0 {
			timeout = string(arg)
		}

		if arg := httpArgs.Peek("query"); len(arg) > 0 {
			query = string(arg)
		}

		if arg := httpArgs.Peek("offset"); len(arg) > 0 {
			offset, err = fasthttp.ParseUint(arg)
			if err != nil {
				return nil, err
			}
		}

		limit, err = httpArgs.GetUint("limit")
		if err != nil {
			return nil, err
		}

		return gateway.selectQuery(start, end, timeout, query, offset, limit)
	})
}

//list labels of matched series
func (gateway *Gateway) HttpSeriesLabels(c *fasthttp.RequestCtx) {
	exeHttpQuery(c, func() (interface{}, error) {
		var httpArgs *fasthttp.Args

		if util.YoloString(c.Method()) == "GET" {
			httpArgs = c.QueryArgs()
		} else {
			httpArgs = c.PostArgs()
		}

		var (
			matches             []string
			start, end, timeout string
		)

		if arg := httpArgs.Peek("start"); len(arg) > 0 {
			start = string(arg)
		}

		if arg := httpArgs.Peek("end"); len(arg) > 0 {
			end = string(arg)
		}

		if arg := httpArgs.PeekMulti("match[]"); len(arg) > 0 {
			for _, b := range arg {
				matches = append(matches, util.YoloString(b))
			}
		}

		if arg := c.QueryArgs().Peek("timeout"); len(arg) > 0 {
			timeout = string(arg)
		}

		return gateway.seriesLabels(matches, start, end, timeout)
	})
}

//list values of the matched label
func (gateway *Gateway) HttpLabelValues(c *fasthttp.RequestCtx) {
	exeHttpQuery(c, func() (interface{}, error) {
		name, ok := c.UserValue("name").(string)
		if !ok {
			return nil, errors.New("label name must be provided")
		}

		var constraint, timeout string
		if arg := c.QueryArgs().Peek("constraint"); len(arg) > 0 {
			constraint = string(arg)
		}

		if arg := c.QueryArgs().Peek("timeout"); len(arg) > 0 {
			timeout = string(arg)
		}

		return gateway.labelValues(name, constraint, timeout)
	})
}

func (gateway *Gateway) HttpIngest(jobBase64Encoded bool) func(c *fasthttp.RequestCtx) {
	return func(c *fasthttp.RequestCtx) {
		c.Error("not implements", 404)
	}
}

func (gateway *Gateway) selectQuery(startT, endT, timeout, query string, offset int, limit int) (*queryResult, error) {
	span := opentracing.StartSpan("selectQuery", opentracing.Tag{"query", query})
	defer span.Finish()

	if startT == "" {
		return nil, errors.New("start time must be provided")
	}

	start, err := ParseTime(startT)
	if err != nil {
		return nil, err
	}

	if endT == "" {
		return nil, errors.New("end time must be provided")
	}

	end, err := ParseTime(endT)
	if err != nil {
		return nil, err
	}

	if end.Before(start) {
		return nil, errors.New("end time must not be before start time")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	//if limit > 11000 {
	//	return nil, errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
	//}

	ctx := context.WithValue(context.Background(), "span", span)
	if timeout != "" {
		var cancel context.CancelFunc
		to, err := ParseDuration(timeout)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	if query == "" {
		return nil, errors.New("query must be provided")
	}

	qry := gateway.QueryEngine.NewQuery(gateway.Backend, query, start, end, offset, limit)

	res, err := qry.Exec(ctx)
	if err != nil {
		return nil, err
	}

	return &queryResult{
		ResultType: res.Type(),
		Result:     res,
	}, nil
}

func (gateway *Gateway) seriesLabels(matches []string, startT, endT, timeout string) ([][]msg.Label, error) {
	span := opentracing.StartSpan("seriesLabels")
	defer span.Finish()

	var matcherSets [][]*backendmsg.Matcher
	for _, match := range matches {
		matchers, err := logql.ParseMatchers(match)
		if err != nil {
			return nil, err
		}
		matcherSets = append(matcherSets, matchers)
	}

	if len(matcherSets) == 0 {
		return nil, errors.New("no match[] parameter provided")
	}

	if startT == "" {
		return nil, errors.New("start time must be provided")
	}

	start, err := ParseTime(startT)
	if err != nil {
		return nil, err
	}

	if endT == "" {
		return nil, errors.New("end time must be provided")
	}

	end, err := ParseTime(endT)
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.Background(), "span", span)
	if timeout != "" {
		var cancel context.CancelFunc
		to, err := ParseDuration(timeout)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	q, err := gateway.Backend.Querier(ctx, ts.FromTime(start), ts.FromTime(end))
	if err != nil {
		return nil, err
	}
	defer q.Close()

	var (
		params = &backend.SelectParams{OnlyLabels: true}
		sets   []backend.SeriesSet
	)
	for _, matchers := range matcherSets {
		s, err := q.Select(params, matchers...)
		if err != nil {
			return nil, err
		}
		sets = append(sets, s)
	}

	set := backend.NewMergeSeriesSet(sets)
	var metrics [][]msg.Label

	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		return nil, set.Err()
	}

	return metrics, nil
}

func (gateway *Gateway) labelValues(name, constraint, timeout string) ([]string, error) {
	span := opentracing.StartSpan("labelValues", opentracing.Tag{"name", name}, opentracing.Tag{"constraint", constraint})
	defer span.Finish()

	now := ts.FromTime(time.Now())

	var (
		err        error
		mint, maxt = now, now
		matchers   []*backendmsg.Matcher
	)

	if constraint != "" {
		matchers, err = logql.ParseMatchers(constraint)
		if err != nil {
			return nil, err
		}
	}

	ctx := context.WithValue(context.Background(), "span", span)
	if timeout != "" {
		var cancel context.CancelFunc
		to, err := ParseDuration(timeout)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	q, err := gateway.Backend.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	vals, err := q.LabelValues(name, matchers...)
	if err != nil {
		return nil, err
	}

	return vals, nil
}

func exeHttpQuery(c *fasthttp.RequestCtx, f func() (interface{}, error)) {
	c.SetContentType("application/json; charset=utf-8")

	result, err := f()
	if err != nil {
		c.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	queryRes, err := json.Marshal(&httpResponse{
		Status: "success",
		Data:   result,
	})
	if err != nil {
		c.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	c.SetBody(queryRes)
}

func ParseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func ParseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
