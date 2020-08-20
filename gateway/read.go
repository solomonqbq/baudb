// Copyright 2017 The Prometheus Authors
// Copyright 2019 The Baudtime Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gateway

import (
	"context"
	"fmt"
	"sort"

	"github.com/baudb/baudb/gateway/backend"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/prometheus/common/model"
)

// QueryableClient returns a Queryable which queries the given
// Client to select series sets.
func QueryableClient(c Client) backend.Queryable {
	return backend.QueryableFunc(func(ctx context.Context, mint, maxt int64) (backend.Querier, error) {
		return &querier{
			ctx:    ctx,
			mint:   mint,
			maxt:   maxt,
			client: c,
		}, nil
	})
}

// querier is an adapter to make a Client usable as a Querier.
type querier struct {
	ctx        context.Context
	mint, maxt int64
	client     Client
}

// Select implements Querier and uses the given matchers to read series
// sets from the Client.
func (q *querier) Select(selectParams *backend.SelectParams, matchers ...*backendmsg.Matcher) (backend.SeriesSet, error) {
	selectRequest := &backendmsg.SelectRequest{
		Mint:     q.mint,
		Maxt:     q.maxt,
		Matchers: matchers,
	}
	if selectParams != nil {
		selectRequest.Filters = selectParams.Filters
		selectRequest.Offset = selectParams.Offset
		selectRequest.Limit = selectParams.Limit
		selectRequest.OnlyLabels = selectParams.OnlyLabels
	}
	res, err := q.client.Select(q.ctx, selectRequest)
	if err != nil {
		return nil, err
	}
	return FromQueryResult(res), nil
}

// LabelValues implements Querier and is a noop.
func (q *querier) LabelValues(name string, matchers ...*backendmsg.Matcher) ([]string, error) {
	labelValuesRequest := &backendmsg.LabelValuesRequest{
		Mint:     q.mint,
		Maxt:     q.maxt,
		Name:     name,
		Matchers: matchers,
	}
	res, err := q.client.LabelValues(q.ctx, labelValuesRequest)
	if err != nil {
		return nil, err
	}
	return res.Values, nil
}

// Close implements Querier and is a noop.
func (q *querier) Close() error {
	return nil
}

// FromQueryResult unpacks a QueryResult proto.
func FromQueryResult(res *backendmsg.SelectResponse) backend.SeriesSet {
	series := make([]backend.Series, 0, len(res.Series))
	for _, ts := range res.Series {
		if err := validateLabelsAndMetricName(ts.Labels); err != nil {
			return errSeriesSet{err: err}
		}

		series = append(series, &concreteSeries{
			labels:  ts.Labels,
			samples: ts.Points,
		})
	}
	//TODO
	//sort.Sort(byLabel(series))
	return &concreteSeriesSet{
		series: series,
	}
}

// validateLabelsAndMetricName validates the label names/values and metric names returned from remote read.
func validateLabelsAndMetricName(ls []msg.Label) error {
	for _, l := range ls {
		if l.Name == msg.MetricName && !model.IsValidMetricName(model.LabelValue(l.Value)) {
			return fmt.Errorf("invalid metric name: %v", l.Value)
		}
		if !model.LabelName(l.Name).IsValid() {
			return fmt.Errorf("invalid label name: %v", l.Name)
		}
		if !model.LabelValue(l.Value).IsValid() {
			return fmt.Errorf("invalid label value: %v", l.Value)
		}
	}
	return nil
}

// concreteSeriesSet implements SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []backend.Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() backend.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

// concreteSeries implementes Series.
type concreteSeries struct {
	labels  msg.Labels
	samples msg.Points
}

func (c *concreteSeries) Labels() msg.Labels {
	lset := make(msg.Labels, 0, len(c.labels))
	for _, l := range c.labels {
		lset = append(lset, l)
	}

	sort.Sort(lset)
	return lset
}

func (c *concreteSeries) Iterator() backend.SeriesIterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) backend.SeriesIterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// At implements SeriesIterator.
func (c *concreteSeriesIterator) At() (t int64, v []byte) {
	s := c.series.samples[c.cur]
	return s.T, s.V
}

// Next implements SeriesIterator.
func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

// Err implements SeriesIterator.
func (c *concreteSeriesIterator) Err() error {
	return nil
}

// errSeriesSet implements SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() backend.Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}
