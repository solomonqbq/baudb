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

package shardvisitor

import (
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
	"go.uber.org/multierr"
	"sync"
)

func init() {
	registry["all"] = VisitAll
}

func VisitAll(shard *meta.Shard, op func(node *meta.Node) (resp msg.Message, err error)) (msg.Message, error) {
	var multiErr error
	multiResp := new(msg.MultiResponse)

	var (
		wg  sync.WaitGroup
		mtx sync.Mutex
	)

	for i := 0; i < len(shard.Masters); i++ {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			resp, err := op(shard.Masters[idx])

			mtx.Lock()
			if err != nil {
				multiErr = multierr.Append(multiErr, err)
			} else {
				multiResp.Append(resp)
			}
			mtx.Unlock()
		}(i)
	}

	for i := 0; i < len(shard.Slaves); i++ {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			resp, err := op(shard.Slaves[idx])

			mtx.Lock()
			if err != nil {
				multiErr = multierr.Append(multiErr, err)
			} else {
				multiResp.Append(resp)
			}
			mtx.Unlock()
		}(i)
	}

	wg.Wait()

	return multiResp, multiErr
}
