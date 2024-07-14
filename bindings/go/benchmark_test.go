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

package opendal_test

import (
	"testing"

	"github.com/google/uuid"
)

type Size uint64

const (
	_   = iota
	KiB = 1 << (10 * iota)
	MiB
)

func fromKibibytes(kib uint64) Size {
	return Size(kib * KiB)
}

func fromMebibytes(mib uint64) Size {
	return Size(mib * MiB)
}

func (s Size) Bytes() uint64 {
	return uint64(s)
}

func runBenchmarkWrite(b *testing.B, size Size) {
	path := uuid.NewString()

	data := genFixedBytes(uint(size.Bytes()))

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		err := op.Write(path, data)
		if err != nil {
			b.Fatalf("%s", err)
		}
	}
}

func BenchmarkWrite4KiB(b *testing.B) {
	runBenchmarkWrite(b, fromKibibytes(4))
}

func BenchmarkWrite256KiB(b *testing.B) {
	runBenchmarkWrite(b, fromKibibytes(256))
}

func BenchmarkWrite4MiB(b *testing.B) {
	runBenchmarkWrite(b, fromMebibytes(4))
}

func BenchmarkWrite16MiB(b *testing.B) {
	runBenchmarkWrite(b, fromMebibytes(16))
}

func runBenchmarkRead(b *testing.B, size Size) {
	path := uuid.NewString()

	data := genFixedBytes(uint(size.Bytes()))

	err := op.Write(path, data)

	if err != nil {
		b.Fatalf("%s", err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := op.Read(path)
		if err != nil {
			b.Fatalf("%s", err)
		}
	}
}

func BenchmarkRead4KiB(b *testing.B) {
	runBenchmarkRead(b, fromKibibytes(4))
}

func BenchmarkRead256KiB(b *testing.B) {
	runBenchmarkRead(b, fromKibibytes(256))
}

func BenchmarkRead4MiB(b *testing.B) {
	runBenchmarkRead(b, fromMebibytes(4))
}

func BenchmarkRead16MiB(b *testing.B) {
	runBenchmarkRead(b, fromMebibytes(16))
}
