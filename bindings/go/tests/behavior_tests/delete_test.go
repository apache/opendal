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

func testsDelete(cap *opendal.Capability) []behaviorTest {
	if !cap.Stat() || !cap.Delete() || !cap.Write() {
		return nil
	}
	tests := []behaviorTest{
		testDeleteFile,
		testDeleteEmptyDir,
		testDeleteWithSpecialChars,
		testDeleteNotExisting,
	}
	return tests
}

func testDeleteFile(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, _ := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	assert.Nil(op.Delete(path))

	assert.False(op.IsExist(path))
}

func testDeleteEmptyDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path), "create must succeed")

	assert.Nil(op.Delete(path))
}

func testDeleteWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := uuid.NewString() + " !@#$%^&()_+-=;',.txt"
	path, content, _ := fixture.NewFileWithPath(path)

	assert.Nil(op.Write(path, content), "write must succeed")

	assert.Nil(op.Delete(path))

	assert.False(op.IsExist(path))
}

func testDeleteNotExisting(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := uuid.NewString()

	assert.Nil(op.Delete(path))
}
