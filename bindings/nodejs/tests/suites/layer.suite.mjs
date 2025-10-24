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

import { test, assert } from 'vitest'

import { RetryLayer, ConcurrentLimitLayer } from '../../index.mjs'

/**
 * @param {import("../../index").Operator} op
 */
export function run(op) {
  test('test operator with retry layer', async () => {
    const retryLayer = new RetryLayer()
    retryLayer.maxTimes = 3
    retryLayer.jitter = true

    const layeredOp = op.layer(retryLayer.build())
    await layeredOp.check()
  })

  test('test operator with concurrent limit layer', async () => {
    const concurrentLimitLayer = new ConcurrentLimitLayer(1024)
    const layeredOp = op.layer(concurrentLimitLayer.build())

    await layeredOp.check()
  })

  test('test operator with concurrent limit layer and http permits', async () => {
    const concurrentLimitLayer = new ConcurrentLimitLayer(1024)
    concurrentLimitLayer.httpPermits = 512

    const layeredOp = op.layer(concurrentLimitLayer.build())

    await layeredOp.check()
  })
}
