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
	"github.com/apache/opendal/bindings/go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsWrite(cap *opendal.Capability) []behaviorTest {
	if !cap.Write() || !cap.Stat() || !cap.Read() {
		return nil
	}
	return []behaviorTest{
		testWriteOnly,
		testWriteWithEmptyContent,
		testWriteWithDirPath,
		testWriteWithSpecialChars,
		testWriteOverwrite,
		testWriterWrite,
	}
}

func testWriteOnly(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content))

	meta, err := op.Stat(path)
	assert.Nil(err, "stat must succeed")
	assert.Equal(uint64(size), meta.ContentLength())
}

func testWriteWithEmptyContent(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().WriteCanEmpty() {
		return
	}

	path := fixture.NewFilePath()
	assert.Nil(op.Write(path, []byte{}))

	meta, err := op.Stat(path)
	assert.Nil(err, "stat must succeed")
	assert.Equal(uint64(0), meta.ContentLength())
}

func testWriteWithDirPath(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewDirPath()

	err := op.Write(path, []byte("1"))
	assert.NotNil(err)
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testWriteWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFileWithPath(uuid.NewString() + " !@#$%^&()_+-=;',.txt")

	assert.Nil(op.Write(path, content))

	meta, err := op.Stat(path)
	assert.Nil(err, "stat must succeed")
	assert.Equal(uint64(size), meta.ContentLength())
}

func testWriteOverwrite(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().WriteCanMulti() {
		return
	}

	path := fixture.NewFilePath()
	size := uint(5 * 1024 * 1024)
	contentOne, contentTwo := genFixedBytes(size), genFixedBytes(size)

	assert.Nil(op.Write(path, contentOne))
	bs, err := op.Read(path)
	assert.Nil(err, "read must succeed")
	assert.Equal(contentOne, bs, "read content_one")

	assert.Nil(op.Write(path, contentTwo))
	bs, err = op.Read(path)
	assert.Nil(err, "read must succeed")
	assert.NotEqual(contentOne, bs, "content_one must be overwrote")
	assert.Equal(contentTwo, bs, "read content_two")
}

func testWriterWrite(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().WriteCanMulti() {
		return
	}

	path := fixture.NewFilePath()
	// StdWriter capacity is 256KB
	size := uint(256 * 1024)
	contentA := genFixedBytes(size)
	contentB := genFixedBytes(size)

	w, err := op.Writer(path)
	assert.Nil(err)
	_, err = w.Write(contentA)
	assert.Nil(err)
	_, err = w.Write(contentB)
	assert.Nil(err)
	assert.Nil(w.Close())

	meta, err := op.Stat(path)
	assert.Nil(err, "stat must succeed")
	assert.Equal(uint64(size*2), meta.ContentLength())

	bs, err := op.Read(path)
	assert.Nil(err, "read must succeed")
	assert.Equal(uint64(size*2), uint64(len(bs)), "read size")
	assert.Equal(contentA, bs[:size], "read contentA")
	assert.Equal(contentB, bs[size:], "read contentB")
}
