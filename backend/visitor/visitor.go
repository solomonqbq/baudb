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

package visitor

import (
	"github.com/baudb/baudb/meta"
	"github.com/baudb/baudb/msg"
)

type Visitor func(shard *meta.Shard, op func(node *meta.Node) (resp msg.Message, err error)) (msg.Message, error)

var (
	registry = make(map[string]Visitor)
	NOOP     = func(shard *meta.Shard, op func(node *meta.Node) (resp msg.Message, err error)) (resp msg.Message, err error) {
		return nil, nil
	}
)

func GetVisitor(name string) (v Visitor, found bool) {
	v, found = registry[name]
	return
}
