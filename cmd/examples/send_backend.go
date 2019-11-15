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
	"context"
	"fmt"
	"github.com/baudb/baudb/msg"
	backendmsg "github.com/baudb/baudb/msg/backend"
	"github.com/baudb/baudb/tcp/client"
	"github.com/baudb/baudb/util"
	ts "github.com/baudb/baudb/util/time"
	"time"
)

func main() {
	cli := client.NewBackendClient("name", "localhost:8088", 0, 2)

	var t int64

	s := make([]*msg.Series, 100)
	r := &backendmsg.AddRequest{Series: s}

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
			points := []msg.Point{{t, util.YoloBytes(fmt.Sprintf("add money: %v, haha", i+j*100))}}

			r.Series[i] = &msg.Series{
				Labels: lbs,
				Points: points,
			}

			time.Sleep(time.Millisecond)
		}

		cli.AsyncRequest(r, nil)
	}

	fmt.Println("write complete")

	time.Sleep(3 * time.Second)

	max := time.Now()
	min := max.Add(-100 * time.Second)

	response, err := cli.SyncRequest(context.Background(), &backendmsg.SelectRequest{
		Mint: ts.FromTime(min),
		Maxt: ts.FromTime(max),
		Matchers: []*backendmsg.Matcher{
			{
				Type:  backendmsg.MatchEqual,
				Name:  "__name__",
				Value: "test",
			},
		},
	})
	if err != nil {
		panic(err)
	}

	for _, series := range response.(*backendmsg.SelectResponse).Series {
		for _, point := range series.Points {
			fmt.Println(point.T, point.V)
		}
	}
}
