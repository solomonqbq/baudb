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
	"go.uber.org/multierr"
	"math/rand"
	"time"

	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func init() {
	registry["random"] = VisitRandom
}

func VisitRandom(shard *meta.Shard, op func(node *meta.Node) (resp msg.Message, err error)) (resp msg.Message, err error) {
	var multiErr error

	masterSize := len(shard.Masters)
	slaveSize := len(shard.Slaves)

	i := random.Intn(masterSize + slaveSize)

	if i < masterSize {
		if resp, err = op(shard.Masters[i]); err != nil {
			multiErr = multierr.Append(multiErr, err)
		} else {
			return
		}
	} else {
		if resp, err = op(shard.Slaves[i-masterSize]); err != nil {
			multiErr = multierr.Append(multiErr, err)
		} else {
			return
		}
	}

	return nil, multiErr
}
