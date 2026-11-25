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
import { beforeEach, describe, expect, it } from 'vitest'

import { Operator } from '../index.mjs'

const path = 'options-bigint'
const content = Buffer.from('0123456789abcdef')

// BigInt values that cannot be represented as an unsigned 64-bit integer.
// Accepting them silently dropped the sign and the high bits, so -1n became 1
// and 1n << 64n became 0, and reads and writes addressed the wrong bytes
// instead of failing.
const invalidValues = [
  ['negative', -1n],
  ['wider than u64', 1n << 64n],
]

describe('bigint options', () => {
  const op = new Operator('memory')

  beforeEach(async () => {
    await op.write(path, content)
  })

  describe('read', () => {
    for (const field of ['offset', 'size']) {
      for (const [label, value] of invalidValues) {
        it(`read rejects ${field} that is ${label}`, async () => {
          await expect(op.read(path, { [field]: value })).rejects.toThrowError(`${field} must`)
        })

        it(`readSync rejects ${field} that is ${label}`, () => {
          expect(() => op.readSync(path, { [field]: value })).toThrowError(`${field} must`)
        })
      }
    }

    it('accepts a zero offset', async () => {
      expect((await op.read(path, { offset: 0n })).equals(content)).toBe(true)
      expect(op.readSync(path, { offset: 0n }).equals(content)).toBe(true)
    })

    it('accepts an offset and size', async () => {
      expect((await op.read(path, { offset: 3n, size: 4n })).toString()).toBe('3456')
      expect(op.readSync(path, { offset: 3n, size: 4n }).toString()).toBe('3456')
    })

    it('accepts the largest offset', async () => {
      // An offset past the end of the object reads no bytes, it must not wrap.
      const answer = await op.read(path, { offset: 0xffffffffffffffffn })
      expect(answer.length).toBe(0)
      expect(op.readSync(path, { offset: 0xffffffffffffffffn }).length).toBe(0)
    })

    it('accepts a zero gap', async () => {
      const reader = await op.reader(path, { gap: 0n })
      expect((await reader.readChunk(1024)).toString()).toBe(content.toString())
    })

    it('accepts a positive gap', () => {
      const reader = op.readerSync(path, { gap: 1024n })
      const buf = Buffer.alloc(content.length)
      expect(reader.read(buf)).toBe(BigInt(content.length))
      expect(buf.equals(content)).toBe(true)
    })
  })

  describe('write', () => {
    it('accepts a chunk size', async () => {
      await op.write(path, 'chunked', { chunk: 4n })
      expect((await op.read(path)).toString()).toBe('chunked')
    })

    it('accepts a chunk size when streaming', async () => {
      const writer = await op.writer(path, { chunk: 4n })
      await writer.write('chunked')
      await writer.close()
      expect((await op.read(path)).toString()).toBe('chunked')
    })

    it('accepts a chunk size when streaming synchronously', () => {
      const writer = op.writerSync(path, { chunk: 4n })
      writer.write('chunked')
      writer.close()
      expect(op.readSync(path).toString()).toBe('chunked')
    })
  })
})

// An unparsable timestamp used to be dropped, so the conditional read, stat,
// or reader silently ran unconditionally instead of reporting the mistake.
const invalidTimestamps = ['not-a-date', new Date(0).toUTCString()]
// Return the error a promise rejects with, or undefined when it resolves.
async function settle(promise) {
  try {
    await promise
    return undefined
  } catch (err) {
    return err
  }
}

describe('conditional timestamps', () => {
  const op = new Operator('memory')

  beforeEach(async () => {
    await op.write(path, content)
  })

  for (const field of ['ifModifiedSince', 'ifUnmodifiedSince']) {
    for (const value of invalidTimestamps) {
      it(`read rejects an invalid ${field} (${value})`, async () => {
        await expect(op.read(path, { [field]: value })).rejects.toThrowError(`${field} must be an ISO 8601`)
      })

      it(`readSync rejects an invalid ${field} (${value})`, () => {
        expect(() => op.readSync(path, { [field]: value })).toThrowError(`${field} must be an ISO 8601`)
      })

      it(`reader rejects an invalid ${field} (${value})`, async () => {
        await expect(op.reader(path, { [field]: value })).rejects.toThrowError(`${field} must be an ISO 8601`)
      })

      it(`readerSync rejects an invalid ${field} (${value})`, () => {
        expect(() => op.readerSync(path, { [field]: value })).toThrowError(`${field} must be an ISO 8601`)
      })

      it(`stat rejects an invalid ${field} (${value})`, async () => {
        await expect(op.stat(path, { [field]: value })).rejects.toThrowError(`${field} must be an ISO 8601`)
      })

      it(`statSync rejects an invalid ${field} (${value})`, () => {
        expect(() => op.statSync(path, { [field]: value })).toThrowError(`${field} must be an ISO 8601`)
      })
    }
  }

  it('accepts an ISO 8601 timestamp', async () => {
    // The memory service does not support conditional reads and stats, so the
    // option must reach the service instead of being rejected as malformed.
    const readError = await settle(op.read(path, { ifModifiedSince: '2024-01-01T00:00:00Z' }))
    expect(String(readError?.message ?? '')).not.toMatch('must be an ISO 8601')

    const statError = await settle(op.stat(path, { ifUnmodifiedSince: '2024-01-01T00:00:00Z' }))
    expect(String(statError?.message ?? '')).not.toMatch('must be an ISO 8601')
  })
})

// N-API truncates fractional numbers and wraps negative ones when it converts
// them to the integer behind these options, so a negative `concurrent` silently
// became the largest possible count and `1.5` became `1`.
const invalidCounts = [-1, 1.5, NaN, Infinity, 2 ** 32]

describe('numeric options', () => {
  const op = new Operator('memory')

  beforeEach(async () => {
    await op.write(path, content)
  })

  describe('read', () => {
    for (const value of invalidCounts) {
      it(`read rejects concurrent ${value}`, async () => {
        await expect(op.read(path, { concurrent: value })).rejects.toThrowError(`concurrent must be an integer`)
      })

      it(`readSync rejects concurrent ${value}`, () => {
        expect(() => op.readSync(path, { concurrent: value })).toThrowError(`concurrent must be an integer`)
      })
    }

    it('accepts a zero concurrent and chunk', async () => {
      // Zero keeps the sequential default.
      expect((await op.read(path, { concurrent: 0, chunk: 0 })).equals(content)).toBe(true)
      expect(op.readSync(path, { concurrent: 0, chunk: 0 }).equals(content)).toBe(true)
    })

    it('accepts a positive concurrent and the largest chunk', async () => {
      expect((await op.read(path, { concurrent: 8, chunk: 4294967295 })).equals(content)).toBe(true)
      expect(op.readSync(path, { concurrent: 8, chunk: 4294967295 }).equals(content)).toBe(true)
    })
  })

  describe('reader', () => {
    for (const value of invalidCounts) {
      it(`reader rejects concurrent ${value}`, async () => {
        await expect(op.reader(path, { concurrent: value })).rejects.toThrowError(`concurrent must be an integer`)
      })

      it(`readerSync rejects concurrent ${value}`, () => {
        expect(() => op.readerSync(path, { concurrent: value })).toThrowError(`concurrent must be an integer`)
      })
    }

    it('accepts a zero concurrent and prefetch', async () => {
      // Zero keeps the sequential default and disables prefetching.
      const reader = await op.reader(path, { concurrent: 0, prefetch: 0 })
      expect((await reader.readChunk(1024)).toString()).toBe(content.toString())
    })

    it('accepts a positive concurrent, chunk and prefetch', async () => {
      const reader = await op.reader(path, { concurrent: 2, chunk: 4, prefetch: 1 })
      const output = []
      let chunk
      while ((chunk = await reader.readChunk(1024)) !== null) {
        output.push(chunk)
      }
      expect(Buffer.concat(output).equals(content)).toBe(true)
    })
  })

  describe('list', () => {
    it('accepts a zero and a positive limit', async () => {
      const zero = await op.list('/', { limit: 0 })
      expect(zero.some((entry) => entry.path() === path)).toBe(true)

      const positive = await op.list('/', { limit: 1024 })
      expect(positive.some((entry) => entry.path() === path)).toBe(true)
    })
  })

  describe('write', () => {
    for (const value of invalidCounts) {
      it(`write rejects concurrent ${value}`, async () => {
        await expect(op.write(path, 'hello', { concurrent: value })).rejects.toThrowError(
          'concurrent must be an integer',
        )
      })

      it(`writeSync rejects concurrent ${value}`, () => {
        expect(() => op.writeSync(path, 'hello', { concurrent: value })).toThrowError('concurrent must be an integer')
      })

      it(`writer rejects concurrent ${value}`, async () => {
        await expect(op.writer(path, { concurrent: value })).rejects.toThrowError('concurrent must be an integer')
      })

      it(`writerSync rejects concurrent ${value}`, () => {
        expect(() => op.writerSync(path, { concurrent: value })).toThrowError('concurrent must be an integer')
      })
    }

    it('accepts a zero concurrent', async () => {
      await op.write(path, 'sequential', { concurrent: 0 })
      expect((await op.read(path)).toString()).toBe('sequential')
    })
  })
})

// `expires` was converted straight from a `u32`, so a negative value became a
// request that stays valid for more than a century.
describe('presign expires', () => {
  const op = new Operator('memory')

  for (const method of ['presignRead', 'presignWrite', 'presignStat', 'presignDelete']) {
    for (const value of invalidCounts) {
      it(`${method} rejects expires ${value}`, async () => {
        await expect(op[method](path, value)).rejects.toThrowError('expires must be an integer')
      })
    }

    it(`${method} accepts a positive expires`, async () => {
      // The memory service does not support presign, so the value must reach
      // the service instead of being rejected as malformed.
      const error = await settle(op[method](path, 3600))
      expect(String(error?.message ?? '')).not.toMatch('expires must be an integer')
    })
  }
})
