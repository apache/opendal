/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package opendal

type BoundType int

const (
	Included BoundType = iota
	Excluded
	Unbounded
)

type Bound struct {
	Type  BoundType
	Value uint64
}

func (b Bound) GetValue() uint64 {
	switch b.Type {
	case Included, Excluded:
		return b.Value
	default:
		return 0
	}
}

type Range struct {
	start Bound
	end   Bound
}

func (r Range) Start() uint64 {
	return r.start.GetValue()
}

func (r Range) End() uint64 {
	return r.end.GetValue()
}

func rangeFrom(value uint64) *Range {
	return &Range{
		start: Bound{Type: Included, Value: value},
		end:   Bound{Type: Unbounded},
	}
}

func rangeUntil(value uint64) *Range {
	return &Range{
		start: Bound{Type: Unbounded},
		end:   Bound{Type: Excluded, Value: value},
	}
}

func rangeBetween(start, end uint64) *Range {
	return &Range{
		start: Bound{Type: Included, Value: start},
		end:   Bound{Type: Excluded, Value: end},
	}
}
