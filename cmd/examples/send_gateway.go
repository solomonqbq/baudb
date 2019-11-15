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

package main

import (
	"fmt"
	"github.com/baudb/baudb/msg"
	"github.com/baudb/baudb/msg/gateway"
	"github.com/baudb/baudb/tcp/client"
	"github.com/baudb/baudb/util"
	ts "github.com/baudb/baudb/util/time"
	"time"
)

func main() {
	addrProvider := client.NewStaticAddrProvider("localhost:8088")
	cli := client.NewGatewayClient("name", addrProvider)

	var t int64

	s := make([]*msg.Series, 100)
	r := &gateway.AddRequest{Series: s}

	for j := 0; j < 1000; j++ {
		for i := 0; i < 100; i++ {
			lbs := []msg.Label{
				{"__name__", "test"},
				{"host", "localhost"},
				{"to", "backend"},
				{"idc", "langfang"},
				{"state", "0"},
			}

			t = ts.FromTime(time.Now())
			points := []msg.Point{{t, util.YoloBytes(fmt.Sprintf("add 钱: %v, haha", i+j*100))}}

			r.Series[i] = &msg.Series{
				Labels: lbs,
				Points: points,
			}

			time.Sleep(time.Millisecond)
		}

		cli.AsyncRequest(r, nil)
	}

	fmt.Println("write complete")
}
