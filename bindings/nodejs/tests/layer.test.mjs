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

// N-API used to hand these values to the Rust layer unchanged: `-1` wrapped to
// the largest count, `1.5` was truncated to `1`, and `2 ** 32` wrapped to `0`.
const invalidCounts = [-1, 1.5, NaN, Infinity, 2 ** 32]

// Values that used to become a zero duration, which arms an immediate deadline,
// or a duration the core layer cannot represent.
const invalidMillis = [-1, -0.5, NaN, Infinity, -Infinity, 1e30]

describe('ThrottleLayer', () => {
  // A fractional rate was truncated to whole bytes and a negative rate wrapped
  // to the largest one, which silently changed or disabled the rate limit.
  for (const value of [0, ...invalidCounts]) {
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
  // A zero-permit semaphore never hands out a permit, so every operation hangs
  // forever, and a negative permit count wraps to the largest count and
  // disables the limit instead of failing.
  for (const permits of [0, ...invalidCounts]) {
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
  // A negative retry count wrapped to the largest count, so retrying never
  // stopped, and a fractional one was truncated to fewer retries.
  for (const maxTimes of invalidCounts) {
    test(`rejects maxTimes ${maxTimes}`, () => {
      const layer = new RetryLayer()
      assertInvalidArgument(() => {
        layer.maxTimes = maxTimes
      }, 'maxTimes')
    })
  }

  test('accepts a zero maxTimes', () => {
    // Zero retries is a valid way to disable retrying.
    const layer = new RetryLayer()
    layer.maxTimes = 0
    assert.ok(layer.build())
  })

  test('accepts a positive maxTimes', () => {
    const layer = new RetryLayer()
    layer.maxTimes = 3
    assert.ok(layer.build())
  })

  // A factor below one makes the backoff shrink, and a non-finite one or a
  // value beyond `f32` produces a delay that the core layer cannot represent.
  for (const factor of [-1, 0, 0.5, NaN, Infinity, -Infinity, 1e39]) {
    test(`rejects factor ${factor}`, () => {
      const layer = new RetryLayer()
      assertInvalidArgument(() => {
        layer.factor = factor
      }, 'factor')
    })
  }

  test('accepts valid factors', () => {
    for (const factor of [1, 1.5, 2, 3.4028234663852886e38]) {
      const layer = new RetryLayer()
      layer.factor = factor
      assert.ok(layer.build())
    }
  })

  for (const field of ['minDelay', 'maxDelay']) {
    for (const value of invalidMillis) {
      test(`rejects ${field} ${value}`, () => {
        const layer = new RetryLayer()
        assertInvalidArgument(() => {
          layer[field] = value
        }, field)
      })
    }

    test(`accepts non-negative ${field}`, () => {
      // A zero delay is valid, it retries without waiting.
      for (const value of [0, 1, 0.5, 60000]) {
        const layer = new RetryLayer()
        layer[field] = value
        assert.ok(layer.build())
      }
    })
  }
})

describe('TimeoutLayer', () => {
  // A negative or `NaN` timeout became zero milliseconds, which timed out every
  // operation immediately, and `Infinity` became a deadline a lifetime away.
  for (const field of ['timeout', 'ioTimeout']) {
    for (const value of [0, 1e-7, ...invalidMillis]) {
      test(`rejects ${field} ${value}`, () => {
        const layer = new TimeoutLayer()
        assertInvalidArgument(() => {
          layer[field] = value
        }, field)
      })
    }

    test(`accepts a positive ${field}`, () => {
      for (const value of [1, 0.5, 10000]) {
        const layer = new TimeoutLayer()
        layer[field] = value
        assert.ok(layer.build())
      }
    })
  }
})
