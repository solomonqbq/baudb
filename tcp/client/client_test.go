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

package client

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/baudb/baudb/msg"
	gatewaymsg "github.com/baudb/baudb/msg/gateway"
)

var staticAddrProvider = NewStaticAddrProvider("127.0.0.1:8087", "127.0.0.1:8087")

func TestBaudClient_RangeQuery(t *testing.T) {
	c := NewGatewayClient("client_test_proxy", staticAddrProvider)
	defer c.Close()

	now := time.Now().UnixNano() / 1e9
	req := &gatewaymsg.SelectRequest{
		Query:   "{host=\"localhost\"}",
		Start:   strconv.FormatInt(now-180, 10),
		End:     strconv.FormatInt(now, 10),
		Timeout: "3",
	}

	resp, err := c.SyncRequest(context.Background(), req)
	if err != nil {
		t.Error(err)
		return
	}

	response, ok := resp.(*gatewaymsg.SelectResponse)
	if !ok {
		t.Log("bad response type")
		t.Fail()
		return
	}

	if response.Status == msg.StatusCode_Failed {
		t.Log("status code shows failed")
		t.Fail()
		return
	}

	t.Log(response.Result)
}

func TestBaudClient_Write(t *testing.T) {
	c := NewGatewayClient("client_test_proxy", staticAddrProvider)
	defer c.Close()

	now := time.Now().UnixNano() / 1e6
	req := &gatewaymsg.AddRequest{
		Series: []*msg.Series{{
			Labels: []msg.Label{
				{"__name__", "test"},
				{"host", "localhost"},
				{"app", "proxy"},
				{"idc", "langfang"},
				{"state", "0"},
			},
			Points: []msg.Point{
				{now - 2, []byte{5}},
				{now - 1, []byte{1}},
				{now, []byte{3}},
			},
		}},
	}

	c.AsyncRequest(req, nil)
}

func TestBaudClient_WriteAndQuery(t *testing.T) {
	c := NewGatewayClient("client_test_proxy", staticAddrProvider)
	defer c.Close()

	for i := 0; i < 30; i++ {
		now := time.Now().UnixNano() / 1e6
		req := &gatewaymsg.AddRequest{
			Series: []*msg.Series{{
				Labels: []msg.Label{
					{"__name__", "test"},
					{"host", "localhost"},
					{"app", "proxy"},
					{"idc", "langfang"},
					{"state", "0"},
				},
				Points: []msg.Point{
					{now - 1, []byte{1}},
					{now, []byte{3}},
				},
			}},
		}

		c.AsyncRequest(req, nil)

		t.Log(i)
		time.Sleep(2 * time.Second)
	}

	now := time.Now().UnixNano() / 1e9
	req := &gatewaymsg.SelectRequest{
		Query:   "{host=\"localhost\"}",
		Start:   strconv.FormatInt(now-180, 10),
		End:     strconv.FormatInt(now, 10),
		Timeout: "3",
	}

	resp, err := c.SyncRequest(context.Background(), req)
	if err != nil {
		t.Error(err)
		return
	}

	response, ok := resp.(*gatewaymsg.SelectResponse)
	if !ok {
		t.Log("bad response type")
		t.Fail()
		return
	}

	if response.Status == msg.StatusCode_Failed {
		t.Log("status code shows failed")
		t.Fail()
		return
	}

	t.Log(response.Result)
}
