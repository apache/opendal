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

import { assert, test } from 'vitest'

import { ConcurrentLimitLayer, ThrottleLayer } from '../index.mjs'

function assertInvalidArgument(createLayer, field) {
  let error
  try {
    createLayer()
  } catch (err) {
    error = err
  }

  assert.instanceOf(error, Error)
  assert.match(error.message, new RegExp(field))
}

// N-API converts these values to 0 when it reads the u32 constructor arguments.
const zeroValues = [0, 2 ** 32, NaN, Infinity]

for (const bandwidth of zeroValues) {
  test(`ThrottleLayer rejects bandwidth ${bandwidth}`, () => {
    assertInvalidArgument(() => new ThrottleLayer(bandwidth, 1024), 'bandwidth')
  })
}

for (const burst of zeroValues) {
  test(`ThrottleLayer rejects burst ${burst}`, () => {
    assertInvalidArgument(() => new ThrottleLayer(1024, burst), 'burst')
  })
}

test('ThrottleLayer accepts positive bandwidth and burst', () => {
  const layer = new ThrottleLayer(1024, 2048)
  assert.ok(layer.build())
})

// A zero-permit semaphore never hands out a permit, so every operation hangs
// forever, and a negative permit count wraps to usize::MAX and disables the
// limit instead of failing.
const invalidPermits = [0, -1]

for (const permits of invalidPermits) {
  test(`ConcurrentLimitLayer rejects ${permits} permits`, () => {
    assertInvalidArgument(() => new ConcurrentLimitLayer(permits), 'permits')
  })

  test(`ConcurrentLimitLayer rejects ${permits} httpPermits`, () => {
    const layer = new ConcurrentLimitLayer(1024)
    assertInvalidArgument(() => {
      layer.httpPermits = permits
    }, 'httpPermits')
  })
}

test('ConcurrentLimitLayer accepts positive permits', () => {
  const layer = new ConcurrentLimitLayer(1024)
  layer.httpPermits = 512
  assert.ok(layer.build())
})

test('ConcurrentLimitLayer stays usable after a rejected httpPermits', () => {
  const layer = new ConcurrentLimitLayer(1024)
  assert.throws(() => {
    layer.httpPermits = 0
  })
  assert.ok(layer.build())
})
