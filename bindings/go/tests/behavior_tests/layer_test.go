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
	"time"

	"github.com/apache/opendal/bindings/go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsLayer(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() {
		return nil
	}
	return []behaviorTest{
		testOperatorWithRetryLayer,
		testOperatorWithTimeoutLayer,
	}
}

func testOperatorWithRetryLayer(assert *require.Assertions, _ *opendal.Operator, _ *fixture) {
	layeredOp, closeFunc, err := newOperator(opendal.WithRetry(
		opendal.RetryMaxTimes(3),
		opendal.RetryFactor(2),
		opendal.RetryJitter(),
		opendal.RetryMinDelay(time.Second),
		opendal.RetryMaxDelay(time.Minute),
	))
	assert.NoError(err)
	defer closeFunc()

	assert.NotNil(layeredOp.Info())

	path := uuid.NewString()
	content := []byte("Hello, OpenDAL retry layer!")

	assert.NoError(layeredOp.Write(path, content))
	defer func() {
		_ = layeredOp.Delete(path)
	}()

	got, err := layeredOp.Read(path)
	assert.NoError(err)
	assert.Equal(content, got)
}

func testOperatorWithTimeoutLayer(assert *require.Assertions, _ *opendal.Operator, _ *fixture) {
	layeredOp, closeFunc, err := newOperator(opendal.WithTimeout(time.Minute, 10*time.Second))
	assert.NoError(err)
	defer closeFunc()

	assert.NotNil(layeredOp.Info())

	path := uuid.NewString()
	content := []byte("Hello, OpenDAL timeout layer!")

	assert.NoError(layeredOp.Write(path, content))
	defer func() {
		_ = layeredOp.Delete(path)
	}()

	got, err := layeredOp.Read(path)
	assert.NoError(err)
	assert.Equal(content, got)
}
