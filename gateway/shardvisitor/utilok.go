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
)

func init() {
	registry["utilok"] = UtilOK
}

func UtilOK(shard *meta.Shard, op func(node *meta.Node) (resp msg.Message, err error)) (resp msg.Message, err error) {
	var multiErr error

	nodes := shard.Slaves
	if len(nodes) > 0 {
		for i := len(nodes) - 1; i >= 0; i-- {
			if resp, err = op(nodes[i]); err != nil {
				multiErr = multierr.Append(multiErr, err)
			} else {
				return
			}
		}
	}

	nodes = shard.Masters
	if len(nodes) > 0 {
		for i := len(nodes) - 1; i >= 0; i-- {
			if resp, err = op(nodes[i]); err != nil {
				multiErr = multierr.Append(multiErr, err)
			} else {
				return
			}
		}
	}

	return nil, multiErr
}
