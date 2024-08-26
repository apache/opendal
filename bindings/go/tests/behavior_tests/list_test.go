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
	"slices"
	"strings"

	"github.com/apache/opendal/bindings/go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsList(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() || !cap.List() {
		return nil
	}
	return []behaviorTest{
		testListCheck,
		testListDir,
		testListPrefix,
		testListRichDir,
		testListEmptyDir,
		testListNonExistDir,
		testListSubDir,
		testListNestedDir,
		testListDirWithFilePath,
	}
}

func testListCheck(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	assert.Nil(op.Check(), "operator check must succeed")
}

func testListDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	path, content, size := fixture.NewFileWithPath(fmt.Sprintf("%s%s", parent, uuid.NewString()))

	assert.Nil(op.Write(path, content), "write must succeed")

	obs, err := op.List(parent)
	assert.Nil(err)
	defer obs.Close()

	var found bool
	for obs.Next() {
		entry := obs.Entry()

		if entry.Path() != path {
			continue
		}

		meta, err := op.Stat(entry.Path())
		assert.Nil(err)
		assert.True(meta.IsFile())
		assert.Equal(uint64(size), meta.ContentLength())
		found = true
		break
	}
	assert.Nil(obs.Error())
	assert.True(found, "file must be found in list")
}

func testListPrefix(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, _ := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	obs, err := op.List(path[:len(path)-1])
	assert.Nil(err)
	defer obs.Close()
	assert.True(obs.Next())
	assert.Nil(obs.Error())

	entry := obs.Entry()
	assert.Equal(path, entry.Path())
}

func testListRichDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	assert.Nil(op.CreateDir(parent))

	var expected []string
	for range 10 {
		path, content, _ := fixture.NewFileWithPath(fmt.Sprintf("%s%s", parent, uuid.NewString()))
		expected = append(expected, path)
		assert.Nil(op.Write(path, content))
	}

	obs, err := op.List(parent)
	assert.Nil(err)
	defer obs.Close()
	var actual []string
	for obs.Next() {
		entry := obs.Entry()
		actual = append(actual, entry.Path())
	}
	assert.Nil(obs.Error())

	expected = append(expected, parent)
	slices.Sort(expected)
	slices.Sort(actual)

	assert.Equal(expected, actual)
}

func testListEmptyDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	dir := fixture.NewDirPath()

	assert.Nil(op.CreateDir(dir), "create must succeed")

	obs, err := op.List(dir)
	assert.Nil(err)
	defer obs.Close()
	var paths []string
	for obs.Next() {
		entry := obs.Entry()
		paths = append(paths, entry.Path())
		assert.Equal(dir, entry.Path())
	}
	assert.Nil(obs.Error())
	assert.Equal(1, len(paths), "dir should only return itself")

	paths = nil
	obs, err = op.List(strings.TrimSuffix(dir, "/"))
	assert.Nil(err)
	defer obs.Close()
	for obs.Next() {
		entry := obs.Entry()
		path := entry.Path()
		paths = append(paths, path)
		meta, err := op.Stat(path)
		assert.Nil(err, "given dir should exist")
		assert.True(meta.IsDir(), "given dir must be dir, but found: %v", path)
	}
	assert.Nil(obs.Error())
	assert.Equal(1, len(paths), "only return the dir itself, but found: %v", paths)
}

func testListNonExistDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	dir := fixture.NewDirPath()

	obs, err := op.List(dir)
	assert.Nil(err)
	defer obs.Close()
	assert.False(obs.Next(), "dir should only return empty")
}

func testListSubDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path), "create must succeed")

	obs, err := op.List("/")
	assert.Nil(err)
	defer obs.Close()

	var found bool
	for obs.Next() {
		entry := obs.Entry()
		if path != entry.Path() {
			continue
		}
		found = true
		break
	}
	assert.Nil(obs.Error())
	assert.True(found, "dir should be found in list")
}

func testListNestedDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	dir := fixture.PushPath(fmt.Sprintf("%s%s/", parent, uuid.NewString()))

	filePath := fixture.PushPath(fmt.Sprintf("%s%s", dir, uuid.NewString()))
	dirPath := fixture.PushPath(fmt.Sprintf("%s%s/", dir, uuid.NewString()))

	assert.Nil(op.CreateDir(dir), "create must succeed")
	assert.Nil(op.Write(filePath, []byte("test_list_nested_dir")), "write must succeed")
	assert.Nil(op.CreateDir(dirPath), "create must succeed")

	obs, err := op.List(parent)
	assert.Nil(err)
	defer obs.Close()
	var paths []string
	for obs.Next() {
		entry := obs.Entry()
		paths = append(paths, entry.Path())
		assert.Equal(dir, entry.Path())
	}
	assert.Nil(obs.Error())
	assert.Equal(1, len(paths), "parent should only got 1 entry")

	obs, err = op.List(dir)
	assert.Nil(err)
	defer obs.Close()
	paths = nil
	var foundFile bool
	var foundDirPath bool
	var foundDir bool
	for obs.Next() {
		entry := obs.Entry()
		paths = append(paths, entry.Path())
		if entry.Path() == filePath {
			foundFile = true
		} else if entry.Path() == dirPath {
			foundDirPath = true
		} else if entry.Path() == dir {
			foundDir = true
		}
	}
	assert.Nil(obs.Error())
	assert.Equal(3, len(paths), "dir should only got 3 entries")

	assert.True(foundDir, "dir should be found in list")

	assert.True(foundFile, "file should be found in list")
	meta, err := op.Stat(filePath)
	assert.Nil(err)
	assert.True(meta.IsFile())
	assert.Equal(uint64(20), meta.ContentLength())

	assert.True(foundDirPath, "dir path should be found in list")
	meta, err = op.Stat(dirPath)
	assert.Nil(err)
	assert.True(meta.IsDir())
}

func testListDirWithFilePath(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	parent := fixture.NewDirPath()
	path, content, _ := fixture.NewFileWithPath(fmt.Sprintf("%s%s", parent, uuid.NewString()))

	assert.Nil(op.Write(path, content))

	obs, err := op.List(strings.TrimSuffix(parent, "/"))
	assert.Nil(err)
	defer obs.Close()

	for obs.Next() {
		entry := obs.Entry()
		assert.Equal(parent, entry.Path())
	}
	assert.Nil(obs.Error())
}
