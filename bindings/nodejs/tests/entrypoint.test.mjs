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

import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'
import { describe, expect, test } from 'vitest'

import * as esm from '../index.mjs'

const require = createRequire(import.meta.url)
const cjs = require('../index.cjs')
const cjsEntryPath = fileURLToPath(new URL('../index.cjs', import.meta.url))
const generatedDtsPath = fileURLToPath(new URL('../generated.d.ts', import.meta.url))

function parseRuntimeExportsFromDts() {
  const source = readFileSync(generatedDtsPath, 'utf8')
  const names = []
  for (const match of source.matchAll(/^export declare (?:class|const enum) (\w+)/gm)) {
    const name = match[1]
    // ExternalObject is a type-only wrapper emitted by napi-rs.
    if (name !== 'ExternalObject') {
      names.push(name)
    }
  }
  return names.sort()
}

describe('entrypoint exports parity', () => {
  const declaredExports = parseRuntimeExportsFromDts()
  const declaredLayers = declaredExports.filter((name) => name.endsWith('Layer') && name !== 'Layer')

  test('ESM and CJS export every runtime symbol declared in generated.d.ts', async () => {
    for (const name of declaredExports) {
      expect(esm[name], `ESM missing ${name}`).toBeDefined()
      expect(cjs[name], `CJS missing ${name}`).toBeDefined()
      expect(cjs[name]).toBe(esm[name])
    }

    const esmKeys = Object.keys(esm)
      .filter((key) => key !== 'default')
      .sort()
    const cjsKeys = Object.keys(cjs).sort()
    expect(cjsKeys).toEqual(esmKeys)
    expect(cjsKeys).toEqual([...declaredExports, 'layers'].sort())

    // Verify Node's static CJS module lexer exposes every named export to ESM importers.
    const cjsViaImport = await import('../index.cjs')
    const cjsImportKeys = Object.keys(cjsViaImport)
      .filter((key) => key !== 'default')
      .sort()
    expect(cjsImportKeys).toEqual(cjsKeys)
  })

  test('ESM and CJS layers namespace expose every layer class', () => {
    expect(Object.keys(esm.layers).sort()).toEqual(declaredLayers)
    expect(Object.keys(cjs.layers).sort()).toEqual(declaredLayers)

    for (const name of declaredLayers) {
      expect(esm.layers[name]).toBe(esm[name])
      expect(cjs.layers[name]).toBe(cjs[name])
    }

    expect(new cjs.layers.CapabilityOverrideLayer('read=false').build()).toBeTruthy()
    expect(new cjs.layers.ConcurrentLimitLayer(4).build()).toBeTruthy()
    expect(new cjs.layers.LoggingLayer().build()).toBeTruthy()
    expect(new cjs.layers.RetryLayer().build()).toBeTruthy()
    expect(new cjs.layers.ThrottleLayer(1024, 2048).build()).toBeTruthy()
    expect(new cjs.layers.TimeoutLayer().build()).toBeTruthy()
  })

  test('CJS stream wrappers handle errors, string encodings, and destroy in isolation', () => {
    // Run in a fresh Node process so index.mjs does not overwrite shared prototypes.
    const script = `
      const assert = require('node:assert/strict')
      const { once } = require('node:events')
      const { finished } = require('node:stream/promises')
      const od = require(${JSON.stringify(cjsEntryPath)})

      ;(async () => {
        const op = new od.Operator('memory')

        // BlockingReadStream error destroys stream and stops _read re-entry.
        const reader = op.readerSync('missing')
        const originalRead = reader.read.bind(reader)
        let readCalls = 0
        reader.read = (buf) => {
          readCalls += 1
          return originalRead(buf)
        }
        const rs = reader.createReadStream()
        let closed = false
        rs.on('close', () => { closed = true })
        const ended = finished(rs)
        rs.resume()
        await assert.rejects(ended, /NotFound/)
        assert.equal(rs.destroyed, true)
        assert.equal(closed, true)
        assert.equal(rs.reader, null)
        assert.equal(rs.read(), null)
        assert.equal(readCalls, 1)

        // WriteStream and BlockingWriteStream respect encodings with decodeStrings: false.
        const expected = Buffer.concat([
          Buffer.from('hello'),
          Buffer.from('hello'),
          Buffer.from('hi', 'utf16le'),
          Buffer.from([0xff]),
          Buffer.from('utf8'),
          Buffer.from('!?'),
        ])

        const asyncWs = (await op.writer('cjs-async.bin')).createWriteStream({ decodeStrings: false })
        asyncWs.write('68656c6c6f', 'hex')
        asyncWs.write('aGVsbG8=', 'base64')
        asyncWs.write('hi', 'utf16le')
        asyncWs.write('\\xff', 'latin1')
        asyncWs.write('utf8', 'utf8')
        asyncWs.write(Buffer.from('!'))
        asyncWs.end(new Uint8Array([63]))
        await finished(asyncWs)
        assert.equal(asyncWs.writer, null)
        assert.deepEqual(await op.read('cjs-async.bin'), expected)

        const syncWs = op.writerSync('cjs-sync.bin').createWriteStream({ decodeStrings: false })
        syncWs.write('68656c6c6f', 'hex')
        syncWs.write('aGVsbG8=', 'base64')
        syncWs.write('hi', 'utf16le')
        syncWs.write('\\xff', 'latin1')
        syncWs.write('utf8', 'utf8')
        syncWs.write(Buffer.from('!'))
        syncWs.end(new Uint8Array([63]))
        await finished(syncWs)
        assert.equal(syncWs.writer, null)
        assert.deepEqual(op.readSync('cjs-sync.bin'), expected)

        // Destroying WriteStream / BlockingWriteStream clears writer without closing.
        for (const ws of [
          (await op.writer('cjs-abort-async.bin')).createWriteStream(),
          op.writerSync('cjs-abort-sync.bin').createWriteStream(),
        ]) {
          const wsClosed = once(ws, 'close')
          ws.destroy()
          await wsClosed
          assert.equal(ws.destroyed, true)
          assert.equal(ws.writer, null)
        }
        assert.equal(await op.exists('cjs-abort-async.bin'), false)
        assert.equal(op.existsSync('cjs-abort-sync.bin'), false)
      })().catch((err) => {
        console.error(err)
        process.exit(1)
      })
    `

    execFileSync(process.execPath, ['-e', script], { stdio: 'inherit' })
  })
})
