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

func testsRead(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() {
		return nil
	}
	return []behaviorTest{
		testReadFull,
		testReader,
		testReadNotExist,
		testReadWithDirPath,
		testReadWithSpecialChars,
	}
}

func testReadFull(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	bs, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(size, uint(len(bs)), "read size")
	assert.Equal(content, bs, "read content")
}

func testReader(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	r, err := op.Reader(path)
	assert.Nil(err)
	defer r.Close()
	bs := make([]byte, size)
	n, err := r.Read(bs)
	assert.Nil(err)
	assert.Equal(size, uint(n), "read size")
	assert.Equal(content, bs[:n], "read content")
}

func testReadNotExist(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewFilePath()

	_, err := op.Read(path)
	assert.NotNil(err)
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
}

func testReadWithDirPath(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path), "create must succeed")

	_, err := op.Read(path)
	assert.NotNil(err)
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testReadWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFileWithPath(uuid.NewString() + " !@#$%^&()_+-=;',.txt")

	assert.Nil(op.Write(path, content), "write must succeed")

	bs, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(size, uint(len(bs)))
	assert.Equal(content, bs)
}
