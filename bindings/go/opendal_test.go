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

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperations(t *testing.T) {
	opts := make(Options)
	opts["root"] = "/myroot"
	operator, err := NewOperator("memory", opts)
	assert.NoError(t, err)
	defer operator.Close()
	err = operator.Write("test", []byte("Hello World from OpenDAL CGO!"))
	assert.NoError(t, err)
	value, err := operator.Read("test")
	assert.NoError(t, err)
	assert.Equal(t, "Hello World from OpenDAL CGO!", string(value))

    println(string(value))
}

func BenchmarkOperator_Write(b *testing.B) {
	opts := make(Options)
	opts["root"] = "/myroot"
	op, _ := NewOperator("memory", opts)
	defer op.Close()

	bs := bytes.Repeat([]byte("a"), 1024*1024)
	op.Write("test", bs)

    b.SetBytes(1024*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Read("test")
	}
}
