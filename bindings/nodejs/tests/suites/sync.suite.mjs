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
import { test } from 'vitest'
import { WriteStream, ReadStream } from '../../index.js'
import { generateFixedBytes } from '../utils.mjs'
import { Readable } from 'node:stream'

export function run(op) {
  describe.runIf(op.capability().blocking)('sync tests', () => {
    test('sync stat not exist files', () => {
      const filename = `random_file_${randomUUID()}`

      try {
        op.statSync(filename)
      } catch (error) {
        assert.include(error.message, 'NotFound')
      }
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
        rs.pipe(ws)

        ws.on('finish', () => {
          const t = op.statSync(filename)
          assert.equal(t.contentLength, buf.length)

          const content = op.readSync(filename)
          assert.equal(Buffer.compare(content, buf), 0) // 0 means equal

          op.deleteSync(filename)
        })
      },
    )

    test.runIf(op.capability().read && op.capability().write)('blocking read stream', async () => {
      let c = generateFixedBytes(3 * 1024 * 1024)
      const filename = `random_file_${randomUUID()}`

      await op.write(filename, c)

      const r = op.readerSync(filename)
      const rs = r.createReadStream()

      let chunks = []
      rs.on('data', (chunk) => {
        chunks.push(chunk)
      })

      rs.on('end', () => {
        const buf = Buffer.concat(chunks)
        assert.equal(Buffer.compare(buf, c), 0)

        op.deleteSync(filename)
      })
    })
  })
}
