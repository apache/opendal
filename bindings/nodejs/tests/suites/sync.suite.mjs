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

import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { Readable } from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import { test } from 'vitest'
import { Operator } from '../../index.mjs'
import { generateFixedBytes } from '../utils.mjs'

export function run(op) {
  describe('sync tests', () => {
    test('sync stat not exist files', () => {
      const filename = `random_file_${randomUUID()}`

      assert.throws(() => op.statSync(filename), /NotFound/)
    })

    test.runIf(op.capability().read && op.capability().write && op.capability().writeCanMulti)(
      'blocking reader/writer stream pipeline',
      async () => {
        const filename = `random_file_${randomUUID()}`
        const buf = generateFixedBytes(5 * 1024 * 1024)
        const rs = Readable.from(buf, {
          highWaterMark: 5 * 1024 * 1024, // to buffer 5MB data to read
        })
        const w = op.writerSync(filename)
        const ws = w.createWriteStream()
        await pipeline(rs, ws)
        await finished(ws)

        const t = op.statSync(filename)
        assert.equal(t.contentLength, BigInt(buf.length))

        const content = op.readSync(filename)
        assert.equal(Buffer.compare(content, buf), 0) // 0 means equal

        op.deleteSync(filename)
      },
    )

    test('a blocking read error destroys the stream', async () => {
      const memory = new Operator('memory')
      const reader = memory.readerSync('missing')
      const originalRead = reader.read.bind(reader)
      let readCalls = 0
      reader.read = (buf) => {
        readCalls += 1
        return originalRead(buf)
      }

      const stream = reader.createReadStream()
      let closed = false
      stream.on('close', () => {
        closed = true
      })

      const ended = finished(stream)
      stream.resume()
      await expect(ended).rejects.toThrow('NotFound')
      expect(stream.destroyed).toBe(true)
      expect(stream.errored?.message).toContain('NotFound')
      expect(closed).toBe(true)
      expect(stream.reader).toBeNull()
      expect(stream.read()).toBeNull()
      expect(readCalls).toBe(1)
    })

    test('blocking write stream respects string encodings with decodeStrings: false', async () => {
      const memory = new Operator('memory')
      const ws = memory.writerSync('encoded.bin').createWriteStream({ decodeStrings: false })
      ws.write('68656c6c6f', 'hex')
      ws.write('aGVsbG8=', 'base64')
      ws.write('hi', 'utf16le')
      ws.write('\xff', 'latin1')
      ws.write('utf8', 'utf8')
      ws.write(Buffer.from('!'))
      ws.end(new Uint8Array([63]))

      await finished(ws)
      expect(ws.writer).toBeNull()

      const expected = Buffer.concat([
        Buffer.from('hello'),
        Buffer.from('hello'),
        Buffer.from('hi', 'utf16le'),
        Buffer.from([0xff]),
        Buffer.from('utf8'),
        Buffer.from('!?'),
      ])
      expect(memory.readSync('encoded.bin')).toEqual(expected)
    })

    test('blocking write stream destroy releases writer without closing', async () => {
      const memory = new Operator('memory')
      const writer = memory.writerSync('aborted.bin')
      let closeCalled = false
      const originalClose = writer.close.bind(writer)
      writer.close = () => {
        closeCalled = true
        return originalClose()
      }

      const ws = writer.createWriteStream()
      const closed = once(ws, 'close')
      ws.destroy()
      await closed

      expect(ws.destroyed).toBe(true)
      expect(ws.writer).toBeNull()
      expect(closeCalled).toBe(false)
      expect(memory.existsSync('aborted.bin')).toBe(false)
    })

    test.runIf(op.capability().read && op.capability().write)('blocking write with behavior', () => {
      let c = Buffer.from('hello world')
      const filename = `random_file_${randomUUID()}`

      const options = { chunk: 1024n * 1024n }
      if (op.capability().writeCanAppend) {
        options.append = true
      }
      if (op.capability().writeWithContentType) {
        options.contentType = 'text/plain'
      }
      if (op.capability().writeWithContentDisposition) {
        options.contentDisposition = 'attachment;filename=test.txt'
      }
      if (op.capability().writeWithCacheControl) {
        options.cacheControl = 'public, max-age=31536000, immutable'
      }

      op.writeSync(filename, c, options)

      if (op.capability().writeCanMulti) {
        const w = op.writerSync(filename, options)
        w.write(c)
        w.close()
      }

      if (op.capability().delete) {
        op.deleteSync(filename)
      }
    })

    test.runIf(op.capability().list)('blocking check', () => {
      op.checkSync()
    })
  })

  const capability = op.capability()
  describe.runIf(capability.read && capability.write && capability.delete)('blocking read streams', () => {
    for (const [name, size, highWaterMark] of [
      ['read with a positive high water mark', 3 * 1024 * 1024, 64 * 1024],
      ['read with a zero high water mark', 11, 0],
      ['read an empty object with a zero high water mark', 0, 0],
    ]) {
      test.runIf(size > 0 || capability.writeCanEmpty)(name, async ({ onTestFinished }) => {
        const content = Buffer.alloc(size, 'hello world')
        const filename = `random_file_${randomUUID()}`
        await op.write(filename, content)
        onTestFinished(() => op.deleteSync(filename))

        const stream = op.readerSync(filename).createReadStream({ highWaterMark })
        const chunks = []
        for await (const chunk of stream) {
          chunks.push(chunk)
        }

        const output = Buffer.concat(chunks)
        expect(output.length).toBe(content.length)
        expect(output.equals(content)).toBe(true)
        expect(stream.readableEnded).toBe(true)
      })
    }
  })
}
