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
import { createServer } from 'node:http'
import { Readable, Writable } from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import { setImmediate } from 'node:timers/promises'
import { Worker } from 'node:worker_threads'
import { onTestFinished, test } from 'vitest'
import { Operator } from '../../index.mjs'
import { generateBytes, generateFixedBytes } from '../utils.mjs'

export function run(op) {
  describe('async tests', () => {
    test('async stat not exist files', async () => {
      const filename = `random_file_${randomUUID()}`

      try {
        await op.stat(filename)
      } catch (error) {
        assert.include(error.message, 'NotFound')
      }
    })

    test.runIf(op.capability().read && op.capability().write && op.capability().writeCanMulti)(
      'reader/writer stream pipeline',
      async () => {
        const filename = `random_file_${randomUUID()}`
        const buf = generateFixedBytes(5 * 1024 * 1024)
        const rs = Readable.from(buf, {
          highWaterMark: 5 * 1024 * 1024, // to buffer 5MB data to read
        })
        const w = await op.writer(filename)
        const ws = w.createWriteStream()
        await pipeline(rs, ws)

        await finished(ws)

        const t = await op.stat(filename)
        assert.equal(t.contentLength, buf.length)

        const content = await op.read(filename)
        assert.equal(Buffer.compare(content, buf), 0) // 0 means equal

        await op.delete(filename)
      },
    )

    test.runIf(op.capability().read && op.capability().write)('read stream', async () => {
      let c = generateFixedBytes(3 * 1024 * 1024)
      const filename = `random_file_${randomUUID()}`

      await op.write(filename, c)

      const r = await op.reader(filename)
      const rs = r.createReadStream()
      let chunks = []
      await pipeline(
        rs,
        new Writable({
          write(chunk, encoding, callback) {
            chunks.push(chunk)
            callback()
          },
        }),
      )

      await finished(rs)
      const buf = Buffer.concat(chunks)
      assert.equal(Buffer.compare(buf, c), 0)

      op.deleteSync(filename)
    })

    test('readChunk owns the bytes returned from a segmented object', async () => {
      const memory = new Operator('memory')
      const writer = await memory.writer('segmented')
      await writer.write(Buffer.from('abc'))
      await writer.write(Buffer.from('defgh'))
      await writer.close()

      const reader = await memory.reader('segmented')
      const merged = await reader.readChunk(4)
      expect(merged).toEqual(Buffer.from('abcd'))
      merged.fill(0)

      const remaining = await reader.readChunk(10)
      expect(remaining).toEqual(Buffer.from('efgh'))
      remaining.fill(0)
      expect(await reader.readChunk(4)).toBeNull()
      expect(await memory.read('segmented')).toEqual(Buffer.from('abcdefgh'))
    })

    test('read and readChunk share a cursor across overlapping calls', async () => {
      const memory = new Operator('memory')
      await memory.write('object', Buffer.from('abcdefghijkl'))
      const reader = await memory.reader('object')
      const first = Buffer.alloc(2)
      expect(await reader.read(first)).toBe(2n)
      expect(first.toString()).toBe('ab')

      for (const size of [0, -1, 1.5, NaN, Infinity, 2 ** 32 + 1]) {
        await expect(reader.readChunk(size)).rejects.toThrow('maxBytes must be an integer')
      }

      const destination = Buffer.alloc(3)
      const [chunk, count] = await Promise.all([reader.readChunk(3), reader.read(destination)])
      expect(count).toBe(3n)
      expect([chunk.toString(), destination.toString()].sort()).toEqual(['cde', 'fgh'])
      expect(await reader.readChunk(0xffffffff)).toEqual(Buffer.from('ijkl'))
      expect(await reader.read(destination)).toBe(0n)
      expect(await reader.readChunk(1)).toBeNull()
    })

    test('overlapping writer writes are serialized without losing data', async () => {
      const memory = new Operator('memory')
      const filename = `writer_concurrency_${randomUUID()}`
      const chunks = Array.from({ length: 64 }, (_, i) =>
        Buffer.from(`chunk-${i.toString().padStart(4, '0')}-${'x'.repeat(64)}`),
      )
      const writer = await memory.writer(filename)

      await Promise.all(chunks.map((chunk) => writer.write(chunk)))
      await writer.close()

      const written = await memory.read(filename)
      expect(written.length).toBe(chunks.length * chunks[0].length)
      const actual = Array.from({ length: chunks.length }, (_, i) =>
        written.subarray(i * chunks[0].length, (i + 1) * chunks[0].length).toString(),
      )
      expect(actual.sort()).toEqual(chunks.map((chunk) => chunk.toString()).sort())

      await memory.delete(filename)
    })

    test('overlapping lister next calls return every entry exactly once', async () => {
      const memory = new Operator('memory')
      const dirname = `lister_concurrency_${randomUUID()}/`
      await memory.createDir(dirname)

      const names = Array.from({ length: 64 }, (_, i) => `file-${i.toString().padStart(4, '0')}`)
      for (const name of names) {
        await memory.write(`${dirname}${name}`, 'content')
      }

      const lister = await memory.lister(dirname)
      const results = await Promise.all(Array.from({ length: names.length + 4 }, () => lister.next()))
      const paths = results.filter((entry) => entry !== null).map((entry) => entry.path())
      expect(paths.sort()).toEqual([dirname, ...names.map((name) => `${dirname}${name}`)].sort())
      expect(results.filter((entry) => entry === null)).toHaveLength(3)

      await memory.removeAll(dirname)
    })

    test('read stream pauses at the high water mark and emits a short tail', async () => {
      const memory = new Operator('memory')
      await memory.write('object', Buffer.from('abcdefghijk'))
      const reader = await memory.reader('object')
      const stream = reader.createReadStream({ highWaterMark: 3 })
      const ended = finished(stream)
      const chunks = []
      let onReadable
      const buffered = new Promise((resolve) => {
        onReadable = resolve
      })
      stream.once('data', () => {
        stream.pause()
        stream.on('readable', onReadable)
      })
      stream.on('data', (chunk) => chunks.push(chunk.toString()))

      await buffered
      await setImmediate()
      expect(chunks).toEqual(['abc'])
      expect(stream.readableLength).toBe(3)
      stream.off('readable', onReadable)
      stream.resume()
      await ended
      expect(chunks).toEqual(['abc', 'def', 'ghi', 'jk'])
    })

    test('destroy discards a read result that arrives after close', async () => {
      const memory = new Operator('memory')
      await memory.write('object', Buffer.from('abc'))
      const reader = await memory.reader('object')
      const readChunk = reader.readChunk.bind(reader)
      let pending
      let release
      const blocked = new Promise((resolve) => {
        release = resolve
      })
      reader.readChunk = (size) => {
        pending = readChunk(size)
        return blocked.then(() => pending)
      }

      const stream = reader.createReadStream({ highWaterMark: 3 })
      const closed = once(stream, 'close')
      stream.read(0)
      stream.destroy()
      await closed
      release()
      expect(await pending).toEqual(Buffer.from('abc'))
      await setImmediate()
      expect(stream.readableLength).toBe(0)
    })

    test('a read error destroys the stream', async () => {
      const memory = new Operator('memory')
      const reader = await memory.reader('missing')
      const stream = reader.createReadStream()
      const ended = finished(stream)
      stream.resume()
      await expect(ended).rejects.toThrow('NotFound')
      expect(stream.destroyed).toBe(true)
    })

    test.each(['index.mjs', 'index.cjs'])(
      '%s releases a pending GET when its worker terminates',
      async (entry) => {
        const server = createServer()
        server.listen(0, '127.0.0.1')
        onTestFinished(() => {
          server.closeAllConnections()
          server.close()
        })
        await once(server, 'listening')
        const requested = once(server, 'request')
        const worker = new Worker(
          `
            const { parentPort, workerData } = require('node:worker_threads')
            import(workerData.entry).then(async ({ Operator }) => {
              const op = new Operator('http', { endpoint: workerData.endpoint })
              const reader = await op.reader('object')
              let retained
              const readChunk = reader.readChunk.bind(reader)
              reader.readChunk = (size) => {
                const pending = readChunk(size)
                if (retained) parentPort.postMessage(retained.toString())
                return pending
              }
              const stream = reader.createReadStream({ highWaterMark: 3 })
              stream.once('data', (chunk) => { retained = chunk })
              stream.resume()
            })
          `,
          {
            eval: true,
            workerData: {
              entry: new URL(`../../${entry}`, import.meta.url).href,
              endpoint: `http://127.0.0.1:${server.address().port}`,
            },
          },
        )
        onTestFinished(() => worker.terminate())

        const message = once(worker, 'message')
        const [, response] = await Promise.race([
          requested,
          once(worker, 'exit').then(([code]) => {
            throw new Error(`Worker exited before starting its GET (${code})`)
          }),
        ])
        const disconnected = once(response, 'close')
        response.writeHead(200, { 'Content-Length': 6 })
        response.write('abc')
        expect(await message).toEqual(['abc'])
        expect(await worker.terminate()).toBe(1)
        await disconnected
      },
      10000,
    )

    test.runIf(op.capability().read && op.capability().write)('write with behavior', async () => {
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
      await op.write(filename, c, options)

      if (op.capability().writeCanMulti) {
        const writer = await op.writer(filename, options)
        await writer.write(c)
        await writer.close()
      }

      if (op.capability().delete) {
        op.deleteSync(filename)
      }
    })

    test.runIf(op.capability().list)('test check', async () => {
      await op.check()
    })
  })
}
