/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gateway

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/baudb/baudb/datanode"
	"github.com/baudb/baudb/gateway/shardvisitor"
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/tcp"
	"github.com/baudb/baudb/tcp/client"
	"github.com/baudb/baudb/vars"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type Client interface {
	Select(ctx context.Context, req *backendmsg.SelectRequest) (*backendmsg.SelectResponse, error)
	LabelValues(ctx context.Context, req *backendmsg.LabelValuesRequest) (*msg.LabelValuesResponse, error)
	Add(ctx context.Context, req *backendmsg.AddRequest) error
	Close() error
	Name() string
}

type clientFactory struct {
	clients sync.Map
}

func (factory *clientFactory) getClient(address string) (*client.Client, error) {
	cli, found := factory.clients.Load(address)
	if !found {
		newCli := client.NewBackendClient("backend_cli_+"+address, address, vars.Cfg.Gateway.SyncConnsPerBackend, vars.Cfg.Gateway.AsyncConnsPerBackend)
		if cli, found = factory.clients.LoadOrStore(address, newCli); found {
			_ = newCli.Close()
		}
	}

	return cli.(*client.Client), nil
}

func (factory *clientFactory) destroy(address string) (err error) {
	cli, found := factory.clients.Load(address)
	if found {
		factory.clients.Delete(address)
		err = cli.(*client.Client).Close()
	}
	return
}

var (
	defaultFactory clientFactory
)

type ShardClient struct {
	shardID  string
	localAPI *datanode.API
	exeQuery shardvisitor.Visitor
	codec    tcp.MsgCodec
}

func (c *ShardClient) Select(ctx context.Context, req *backendmsg.SelectRequest) (*backendmsg.SelectResponse, error) {
	if req == nil {
		return nil, nil
	}

	if parentSpan, ok := ctx.Value("span").(opentracing.Span); ok {
		syncRequest := opentracing.StartSpan("syncRequest", opentracing.ChildOf(parentSpan.Context()))
		syncRequest.SetTag("shard", c.shardID)
		for _, m := range req.Matchers {
			syncRequest.SetTag(m.Name, fmt.Sprintf("%s[%s]", m.Value, m.Type.String()))
		}
		defer syncRequest.Finish()

		carrier := new(bytes.Buffer)
		syncRequest.Tracer().Inject(syncRequest.Context(), opentracing.Binary, carrier)
		req.SpanCtx = carrier.Bytes()
	}

	shard, found := meta.GetShard(c.shardID)
	if !found || shard == nil {
		meta.RefreshTopology()
		return nil, errors.Errorf("no such shard %v", c.shardID)
	}

	resp, err := c.exeQuery(shard, func(node *meta.Node) (msg.Message, error) {
		if c.localAPI != nil && node.IP == vars.LocalIP && node.Port == vars.Cfg.TcpPort {
			return c.localAPI.HandleSelectReq(req), nil
		} else {
			cli, err := defaultFactory.getClient(node.Addr())
			if err != nil {
				return nil, err
			}

			return cli.SyncRequest(ctx, req)
		}
	})

	if err != nil {
		return nil, err
	}

	if selResp, ok := resp.(*backendmsg.SelectResponse); !ok {
		return nil, errors.Wrapf(tcp.BadMsgFormat, "the type of response is '%v'", reflect.TypeOf(resp))
	} else if selResp.Status != msg.StatusCode_Succeed {
		return nil, errors.Errorf("select error:%s", selResp.ErrorMsg)
	} else {
		return selResp, nil
	}
}

func (c *ShardClient) LabelValues(ctx context.Context, req *backendmsg.LabelValuesRequest) (*msg.LabelValuesResponse, error) {
	if req == nil {
		return nil, nil
	}

	if parentSpan, ok := ctx.Value("span").(opentracing.Span); ok {
		syncRequest := opentracing.StartSpan("syncRequest", opentracing.ChildOf(parentSpan.Context()))
		syncRequest.SetTag("shard", c.shardID)
		syncRequest.SetTag("name", req.Name)
		defer syncRequest.Finish()

		carrier := new(bytes.Buffer)
		syncRequest.Tracer().Inject(syncRequest.Context(), opentracing.Binary, carrier)
		req.SpanCtx = carrier.Bytes()
	}

	shard, found := meta.GetShard(c.shardID)
	if !found || shard == nil {
		meta.RefreshTopology()
		return nil, errors.Errorf("no such shard %v", c.shardID)
	}

	resp, err := c.exeQuery(shard, func(node *meta.Node) (msg.Message, error) {
		if c.localAPI != nil && node.IP == vars.LocalIP && node.Port == vars.Cfg.TcpPort {
			return c.localAPI.HandleLabelValuesReq(req), nil
		} else {
			cli, err := defaultFactory.getClient(node.Addr())
			if err != nil {
				return nil, err
			}

			return cli.SyncRequest(ctx, req)
		}
	})

	if err != nil {
		return nil, err
	}

	if lValsResp, ok := resp.(*msg.LabelValuesResponse); !ok {
		return nil, errors.Wrapf(tcp.BadMsgFormat, "the type of response is '%v'", reflect.TypeOf(resp))
	} else if lValsResp.Status != msg.StatusCode_Succeed {
		return nil, errors.Errorf("label values error:%s", lValsResp.ErrorMsg)
	} else {
		return lValsResp, nil
	}
}

func (c *ShardClient) Add(ctx context.Context, req *backendmsg.AddRequest) error {
	if req == nil {
		return nil
	}

	shard, found := meta.GetShard(c.shardID)
	if !found || shard == nil {
		meta.RefreshTopology()
		return errors.Errorf("no such shard %v", c.shardID)
	}

	resp, err := shardvisitor.VisitAllMasters(shard, func(node *meta.Node) (msg.Message, error) {
		if c.localAPI != nil && node.IP == vars.LocalIP && node.Port == vars.Cfg.TcpPort {
			return c.localAPI.HandleAddReq(req), nil
		}

		cli, err := defaultFactory.getClient(node.Addr())
		if err != nil {
			return nil, err
		}

		if vars.Cfg.Gateway.Appender.AsyncTransfer {
			err = cli.AsyncRequest(req, nil)
			if err != nil {
				return nil, err
			}
			return &msg.GeneralResponse{}, nil
		}

		return cli.SyncRequest(ctx, req)
	})

	if err != nil {
		return err
	}

	multiResp, ok := resp.(*msg.MultiResponse)
	if !ok {
		return errors.Wrapf(tcp.BadMsgFormat, "the type of response is '%v'", reflect.TypeOf(resp))
	}

	var multiErr error

	for _, resp := range multiResp.Resp {
		general, ok := resp.(*msg.GeneralResponse)
		if !ok {
			multiErr = multierr.Append(multiErr, errors.Wrapf(tcp.BadMsgFormat, "the type of response is '%v'", reflect.TypeOf(resp)))
		} else if general.Status != msg.StatusCode_Succeed {
			multiErr = multierr.Append(multiErr, errors.New(general.Message))
		}
	}

	return multiErr
}

func (c *ShardClient) Close() error {
	var multiErr error

	masters := meta.GetMasters(c.shardID)
	if len(masters) > 0 {
		for _, master := range masters {
			err := defaultFactory.destroy(master.Addr())
			if err != nil {
				multiErr = multierr.Append(multiErr, err)
			}
		}
	}

	slaves := meta.GetSlaves(c.shardID)
	if len(slaves) > 0 {
		for _, slave := range slaves {
			err := defaultFactory.destroy(slave.Addr())
			if err != nil {
				multiErr = multierr.Append(multiErr, err)
			}
		}
	}

	return multiErr
}

func (c *ShardClient) Name() string {
	return fmt.Sprintf("[ShardClient]@[shard:%s]", c.shardID)
}
