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

import { readFileSync } from 'node:fs'
import path from 'node:path'

import { assert, describe, test } from 'vitest'

import { EntryMode, Metadata, Operator } from '../index.mjs'

// Every `Metadata` member as `generated.d.ts` declared it before `Metadata`
// was generated from core. Existing code relies on each name, member kind,
// and type, so the generated class must keep them all.
const FROZEN_MEMBERS = [
  'isDirectory(): boolean',
  'isFile(): boolean',
  'isDeleted(): boolean',
  'get cacheControl(): string | null',
  'get contentDisposition(): string | null',
  'get contentLength(): bigint | null',
  'get contentEncoding(): string | null',
  'get contentMd5(): string | null',
  'get contentType(): string | null',
  'get userMetadata(): Record<string, string> | null',
  'get etag(): string | null',
  'get lastModified(): string | null',
  'get mode(): EntryMode | null',
  'get version(): string | null',
]

function declaredMembers() {
  const dts = readFileSync(path.resolve(__dirname, '../generated.d.ts'), 'utf8')
  const body = dts.match(/^export declare class Metadata \{\n([\s\S]*?)^\}/m)
  assert.ok(body, 'generated.d.ts declares class Metadata')
  return body[1].split('\n').map((line) => line.trim())
}

function matchesType(value, type) {
  return type.split(' | ').some((t) => {
    switch (t) {
      case 'null':
        return value === null
      case 'boolean':
      case 'string':
      case 'bigint':
        return typeof value === t
      case 'EntryMode':
        return Object.values(EntryMode).includes(value)
      case 'Record<string, string>':
        return typeof value === 'object' && value !== null && Object.values(value).every((v) => typeof v === 'string')
      default:
        throw new Error(`unknown type ${t}`)
    }
  })
}

describe('Metadata keeps its historical surface', async () => {
  const op = new Operator('memory')
  await op.write('file', 'OpenDAL')
  const meta = await op.stat('file')
  const members = declaredMembers()

  for (const member of FROZEN_MEMBERS) {
    const [, getter, name, type] = member.match(/^(get )?(\w+)\(\): (.+)$/)

    test(member, () => {
      assert.include(members, member)

      const descriptor = Object.getOwnPropertyDescriptor(Metadata.prototype, name)
      assert.ok(descriptor, `Metadata.prototype.${name} exists`)
      const value = getter ? meta[name] : meta[name]()
      if (getter) {
        assert.isFunction(descriptor.get, `${name} is a getter`)
      } else {
        assert.isFunction(descriptor.value, `${name} is a method`)
      }
      assert.ok(matchesType(value, type), `${name} returned ${String(value)}, expected ${type}`)
    })
  }
})
