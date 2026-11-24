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

function detachArrayBuffer(ab) {
  if (typeof ab.transfer === 'function') {
    ab.transfer(0)
  } else {
    structuredClone(ab, { transfer: [ab] })
  }
}

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

    test('reader.read rejects when destination buffer is detached while in flight', async () => {
      const payload = Buffer.alloc(1024 * 1024, 0x41)
      const server = createServer()
      server.listen(0, '127.0.0.1')
      onTestFinished(() => {
        server.closeAllConnections()
        server.close()
      })
      await once(server, 'listening')

      const httpOp = new Operator('http', { endpoint: `http://127.0.0.1:${server.address().port}` })
      const reader = await httpOp.reader('object')

      const requested = once(server, 'request')
      const ab = new ArrayBuffer(payload.length)
      const buf = Buffer.from(ab)
      const pending = reader.read(buf)

      const [, response] = await requested
      detachArrayBuffer(ab)
      expect(buf.byteLength).toBe(0)

      response.writeHead(200, { 'Content-Length': payload.length })
      response.end(payload)

      await expect(pending).rejects.toThrow('Destination buffer was detached or shrunk')

      // Detaching a non-empty buffer at EOF also rejects instead of silently returning 0.
      const memory = new Operator('memory')
      await memory.write('empty', Buffer.alloc(0))
      const eofReader = await memory.reader('empty')
      const eofAb = new ArrayBuffer(16)
      const eofBuf = Buffer.from(eofAb)
      const eofPending = eofReader.read(eofBuf)
      detachArrayBuffer(eofAb)
      await expect(eofPending).rejects.toThrow('Destination buffer was detached or shrunk')
    })

    test('reader.read handles resizable buffers, subarrays, and zero-length buffers', async () => {
      const memory = new Operator('memory')
      await memory.write('object', Buffer.from('0123456789abcdef'))
      const reader = await memory.reader('object')

      // Zero-length and already-detached buffers return 0n without advancing the cursor.
      expect(await reader.read(Buffer.alloc(0))).toBe(0n)
      const preDetachedAb = new ArrayBuffer(8)
      const preDetachedBuf = Buffer.from(preDetachedAb)
      detachArrayBuffer(preDetachedAb)
      expect(await reader.read(preDetachedBuf)).toBe(0n)

      // Subarray with non-zero byteOffset only modifies its own slice.
      const backing = Buffer.alloc(16, 0xaa)
      const slice = backing.subarray(4, 10)
      expect(await reader.read(slice)).toBe(6n)
      expect(slice.toString()).toBe('012345')
      expect(backing.subarray(0, 4)).toEqual(Buffer.alloc(4, 0xaa))
      expect(backing.subarray(10, 16)).toEqual(Buffer.alloc(6, 0xaa))

      // Growing a resizable ArrayBuffer while a read is in flight succeeds.
      const growAb = new ArrayBuffer(4, { maxByteLength: 32 })
      const growBuf = Buffer.from(growAb)
      const growPending = reader.read(growBuf)
      growAb.resize(16)
      expect(await growPending).toBe(4n)
      expect(growBuf.subarray(0, 4).toString()).toBe('6789')

      // Shrinking a resizable ArrayBuffer while a read is in flight rejects cleanly.
      const server = createServer()
      server.listen(0, '127.0.0.1')
      onTestFinished(() => {
        server.closeAllConnections()
        server.close()
      })
      await once(server, 'listening')

      const httpOp = new Operator('http', { endpoint: `http://127.0.0.1:${server.address().port}` })
      const httpReader = await httpOp.reader('shrink')
      const requested = once(server, 'request')
      const shrinkAb = new ArrayBuffer(16, { maxByteLength: 32 })
      const shrinkBuf = Buffer.from(shrinkAb)
      const shrinkPending = httpReader.read(shrinkBuf)
      const [, response] = await requested
      shrinkAb.resize(4)
      response.writeHead(200, { 'Content-Length': 16 })
      response.end(Buffer.from('0123456789abcdef'))
      await expect(shrinkPending).rejects.toThrow('Destination buffer was detached or shrunk')

      // Remaining bytes and EOF on the memory reader.
      const tail = Buffer.alloc(10)
      expect(await reader.read(tail)).toBe(6n)
      expect(tail.subarray(0, 6).toString()).toBe('abcdef')
      expect(await reader.read(tail)).toBe(0n)

      // An async read error cleanly releases the rooted destination buffer.
      const missingReader = await memory.reader('missing')
      await expect(missingReader.read(Buffer.alloc(8))).rejects.toThrow('NotFound')
    })

    test('op.write snapshots buffer bytes before returning', async () => {
      const memory = new Operator('memory')

      // Detaching the backing ArrayBuffer immediately after op.write must not UAF or corrupt data.
      const payload = Buffer.alloc(1024 * 1024, 0x42)
      const ab = new ArrayBuffer(payload.length)
      const buf = Buffer.from(ab)
      payload.copy(buf)
      const writePending = memory.write('detached', buf)
      detachArrayBuffer(ab)
      await writePending
      expect(Buffer.compare(await memory.read('detached'), payload)).toBe(0)

      // Mutating a subarray immediately after op.write must not affect the written snapshot.
      const full = Buffer.from('xxxxhello worldyyyy')
      const sub = full.subarray(4, 15)
      const mutatePending = memory.write('mutated', sub)
      full.fill(0)
      await mutatePending
      expect((await memory.read('mutated')).toString()).toBe('hello world')
    })

    test('writer.write snapshots buffer bytes before returning', async () => {
      const memory = new Operator('memory')
      const writer = await memory.writer('streamed')

      const chunkA = Buffer.alloc(512 * 1024, 0x43)
      const abA = new ArrayBuffer(chunkA.length)
      const bufA = Buffer.from(abA)
      chunkA.copy(bufA)
      const pA = writer.write(bufA)
      detachArrayBuffer(abA)
      await pA

      const backingB = Buffer.from('__second-chunk__')
      const subB = backingB.subarray(2, 14)
      const pB = writer.write(subB)
      backingB.fill(0)
      await pB

      await writer.close()

      const written = await memory.read('streamed')
      expect(Buffer.compare(written, Buffer.concat([chunkA, Buffer.from('second-chunk')]))).toBe(0)
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

    test('write stream respects string encodings with decodeStrings: false', async () => {
      const memory = new Operator('memory')
      const writer = await memory.writer('encoded.bin')
      const ws = writer.createWriteStream({ decodeStrings: false })
      ws.write('68656c6c6f', 'hex')
      ws.write('aGVsbG8=', 'base64')
      ws.write('hi', 'utf16le')
      ws.write('\xff', 'latin1')
      ws.write('utf8', 'utf8')
      ws.write(Buffer.from('!'))
      ws.end(new Uint8Array([63]))

      await finished(ws)

      const expected = Buffer.concat([
        Buffer.from('hello'),
        Buffer.from('hello'),
        Buffer.from('hi', 'utf16le'),
        Buffer.from([0xff]),
        Buffer.from('utf8'),
        Buffer.from('!?'),
      ])
      expect(await memory.read('encoded.bin')).toEqual(expected)
    })

    test('write stream destroy releases writer without closing', async () => {
      const memory = new Operator('memory')
      const writer = await memory.writer('aborted.bin')
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
      expect(closeCalled).toBe(false)
      expect(await memory.exists('aborted.bin')).toBe(false)
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
            const load = workerData.entry.endsWith('.cjs')
              ? Promise.resolve(require('node:module').createRequire(new URL(workerData.entry))('opendal'))
              : import(workerData.entry)
            load.then(async ({ Operator }) => {
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
