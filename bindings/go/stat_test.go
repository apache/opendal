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
	"fmt"
	"strings"

	"github.com/apache/opendal/bindings/go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsStat(cap *opendal.Capability) []behaviorTest {
	if !cap.Write() || !cap.Stat() {
		return nil
	}
	return []behaviorTest{
		testStatFile,
		testStatDir,
		testStatNestedParentDir,
		testStatWithSpecialChars,
		testStatNotCleanedPath,
		testStatNotExist,
		testStatRoot,
	}
}

func testStatFile(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content))

	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.True(meta.IsFile())
	assert.Equal(meta.ContentLength(), uint64(size))

	if op.Info().GetFullCapability().CreateDir() {
		_, err := op.Stat(fmt.Sprintf("%s/", path))
		assert.NotNil(err)
		assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
	}
}

func testStatDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	path := fixture.NewDirPath()
	assert.Nil(op.CreateDir(path))

	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.True(meta.IsDir())

	meta, err = op.Stat(strings.TrimSuffix(path, "/"))
	if err != nil {
		assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
	} else {
		assert.True(meta.IsDir())
	}
}

func testStatNestedParentDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	parent := fixture.NewDirPath()
	path, content, _ := fixture.NewFileWithPath(fmt.Sprintf("%s%s", parent, uuid.NewString()))

	assert.Nil(op.Write(path, content), "write must succeed")

	meta, err := op.Stat(parent)
	assert.Nil(err)
	assert.True(meta.IsDir())
}

func testStatWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFileWithPath(uuid.NewString() + " !@#$%^&()_+-=;',.txt")

	assert.Nil(op.Write(path, content), "write must succeed")

	meta, err := op.Stat(path)
	assert.Nil(err)
	assert.True(meta.IsFile())
	assert.Equal(uint64(size), meta.ContentLength())
}

func testStatNotCleanedPath(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	meta, err := op.Stat(fmt.Sprintf("//%s", path))
	assert.Nil(err)
	assert.True(meta.IsFile())
	assert.Equal(uint64(size), meta.ContentLength())
}

func testStatNotExist(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewFilePath()

	_, err := op.Stat(path)
	assert.NotNil(err)
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))

	if op.Info().GetFullCapability().CreateDir() {
		_, err := op.Stat(fmt.Sprintf("%s/", path))
		assert.NotNil(err)
		assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
	}
}

func testStatRoot(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	meta, err := op.Stat("")
	assert.Nil(err)
	assert.True(meta.IsDir())

	meta, err = op.Stat("/")
	assert.Nil(err)
	assert.True(meta.IsDir())

}
