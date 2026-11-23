// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package opendal_test

import (
	"context"
	"fmt"

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
		testDeleterDeleteFiles,
		testDeleterDeleteNotExisting,
		testRemoveFiles,
	}
	if cap.DeleteWithRecursive() {
		tests = append(tests, testDeleteWithRecursive, testDeleterDeleteWithRecursive)
	}
	if cap.DeleteWithVersion() {
		tests = append(tests, testDeleteWithVersion)
	}
	return tests
}

func testDeleteFile(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, _ := fixture.NewFile()

	_, err := op.Write(ctx, path, content)
	assert.Nil(err, "write must succeed")

	assert.Nil(op.Delete(ctx, path))

	assert.False(op.IsExist(ctx, path))
}

func testDeleteEmptyDir(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetCapability().CreateDir() {
		return
	}

	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(ctx, path), "create must succeed")

	assert.Nil(op.Delete(ctx, path))
}

func testDeleteWithSpecialChars(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := uuid.NewString() + " !@#$%^&()_+-=;',.txt"
	path, content, _ := fixture.NewFileWithPath(path)

	_, err := op.Write(ctx, path, content)
	assert.Nil(err, "write must succeed")

	assert.Nil(op.Delete(ctx, path))

	assert.False(op.IsExist(ctx, path))
}

func testDeleteNotExisting(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := uuid.NewString()

	assert.Nil(op.Delete(ctx, path))
}

// writeTestFiles writes n fixture files and returns their paths.
func writeTestFiles(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture, n int) []string {
	var paths []string
	for range n {
		path, content, _ := fixture.NewFile()
		_, err := op.Write(ctx, path, content)
		assert.Nil(err, "write must succeed")
		paths = append(paths, path)
	}
	return paths
}

// setupRecursiveDir creates a directory holding three files and returns it, or
// ok=false when the service cannot create directories.
func setupRecursiveDir(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) (dir string, filePaths []string, ok bool) {
	if !op.Info().GetCapability().CreateDir() {
		return "", nil, false
	}

	dir = fixture.NewDirPath()
	assert.Nil(op.CreateDir(ctx, dir), "create dir must succeed")

	for i := range 3 {
		path, content, _ := fixture.NewFileWithPath(fmt.Sprintf("%sfile-%d.txt", dir, i))
		_, err := op.Write(ctx, path, content)
		assert.Nil(err, "write must succeed")
		filePaths = append(filePaths, path)
	}
	return dir, filePaths, true
}

func assertAllGone(ctx context.Context, assert *require.Assertions, op *opendal.Operator, paths ...string) {
	for _, path := range paths {
		exists, err := op.IsExist(ctx, path)
		assert.NoError(err, "check %q", path)
		assert.False(exists, "path %q must be gone", path)
	}
}

func testDeleteWithRecursive(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	dir, filePaths, ok := setupRecursiveDir(ctx, assert, op, fixture)
	if !ok {
		return
	}

	assert.Nil(op.Delete(ctx, dir, opendal.DeleteWithRecursive(true)))

	assertAllGone(ctx, assert, op, dir)
	assertAllGone(ctx, assert, op, filePaths...)
}

func testDeleterDeleteFiles(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	paths := writeTestFiles(ctx, assert, op, fixture, 3)

	deleter, err := op.Deleter(ctx)
	assert.Nil(err, "create deleter must succeed")
	// Release the deleter even when an assertion below fails the test early.
	defer deleter.Discard()
	for _, path := range paths {
		assert.Nil(deleter.Delete(ctx, path), "queue delete must succeed")
	}
	// The deletions are only guaranteed to have happened once Close returns.
	assert.Nil(deleter.Close(ctx), "close deleter must succeed")

	assertAllGone(ctx, assert, op, paths...)
}

func testDeleterDeleteNotExisting(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	deleter, err := op.Deleter(ctx)
	assert.Nil(err, "create deleter must succeed")
	defer deleter.Discard()

	assert.Nil(deleter.Delete(ctx, uuid.NewString()))
	assert.Nil(deleter.Close(ctx))
}

func testDeleterDeleteWithRecursive(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	dir, filePaths, ok := setupRecursiveDir(ctx, assert, op, fixture)
	if !ok {
		return
	}

	deleter, err := op.Deleter(ctx)
	assert.Nil(err, "create deleter must succeed")
	defer deleter.Discard()
	assert.Nil(deleter.Delete(ctx, dir, opendal.DeleteWithRecursive(true)))
	assert.Nil(deleter.Close(ctx), "close deleter must succeed")

	assertAllGone(ctx, assert, op, dir)
	assertAllGone(ctx, assert, op, filePaths...)
}

func testRemoveFiles(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	paths := writeTestFiles(ctx, assert, op, fixture, 3)

	assert.Nil(op.Remove(ctx, paths), "remove must succeed")

	assertAllGone(ctx, assert, op, paths...)
}

func testDeleteWithVersion(ctx context.Context, assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, _ := fixture.NewFile()

	_, err := op.Write(ctx, path, content)
	assert.Nil(err, "write must succeed")

	meta, err := op.Stat(ctx, path)
	assert.Nil(err)
	version, ok := meta.Version()
	if !ok {
		return
	}

	assert.Nil(op.Delete(ctx, path, opendal.DeleteWithVersion(version)))

	assert.False(op.IsExist(ctx, path))
}
