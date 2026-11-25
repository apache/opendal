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

import { assert, describe, test } from 'vitest'

import { ConcurrentLimitLayer, RetryLayer, ThrottleLayer, TimeoutLayer } from '../index.mjs'

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

describe('ThrottleLayer', () => {
  // These values become zero after N-API converts them to uint32.
  for (const value of [0, NaN, Infinity, 2 ** 32]) {
    test(`rejects bandwidth ${value}`, () => {
      assertInvalidArgument(() => new ThrottleLayer(value, 1024), 'bandwidth')
    })

    test(`rejects burst ${value}`, () => {
      assertInvalidArgument(() => new ThrottleLayer(1024, value), 'burst')
    })
  }

  test('accepts a positive bandwidth and burst', () => {
    const layer = new ThrottleLayer(1024, 2048)
    assert.ok(layer.build())
  })

  test('accepts the largest bandwidth and burst', () => {
    const layer = new ThrottleLayer(4294967295, 4294967295)
    assert.ok(layer.build())
  })
})

describe('ConcurrentLimitLayer', () => {
  for (const permits of [0, 0.5, -1, NaN, Infinity]) {
    test(`rejects ${permits} permits`, () => {
      assertInvalidArgument(() => new ConcurrentLimitLayer(permits), 'permits')
    })

    test(`rejects ${permits} httpPermits`, () => {
      const layer = new ConcurrentLimitLayer(1024)
      assertInvalidArgument(() => {
        layer.httpPermits = permits
      }, 'httpPermits')
    })
  }

  test('accepts positive permits', () => {
    const layer = new ConcurrentLimitLayer(1024)
    layer.httpPermits = 512
    assert.ok(layer.build())
  })

  test('stays usable after a rejected httpPermits', () => {
    const layer = new ConcurrentLimitLayer(1024)
    assert.throws(() => {
      layer.httpPermits = 0
    })
    assert.ok(layer.build())
  })
})

describe('RetryLayer', () => {
  test('builds with retry count, factor and fractional delays', () => {
    const layer = new RetryLayer()
    layer.maxTimes = 3
    layer.factor = 2
    layer.minDelay = 0.5
    layer.maxDelay = 25.5
    assert.ok(layer.build())
  })
})

describe('TimeoutLayer', () => {
  test('builds with fractional timeouts', () => {
    const layer = new TimeoutLayer()
    layer.timeout = 10.5
    layer.ioTimeout = 5.5
    assert.ok(layer.build())
  })
})
