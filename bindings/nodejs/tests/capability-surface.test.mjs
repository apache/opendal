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

import { Capability, Operator } from '../index.mjs'

// Every `Capability` member as `generated.d.ts` declared it before
// `Capability` was generated from core. Existing code relies on each name,
// member kind, and type, so the generated class must keep them all.
const FROZEN_MEMBERS = [
  'get stat(): boolean',
  'get statWithIfMatch(): boolean',
  'get statWithIfNoneMatch(): boolean',
  'get statWithIfModifiedSince(): boolean',
  'get statWithIfUnmodifiedSince(): boolean',
  'get statWithVersion(): boolean',
  'get statWithOverrideContentType(): boolean',
  'get statWithOverrideCacheControl(): boolean',
  'get statWithOverrideContentDisposition(): boolean',
  'get read(): boolean',
  'get readWithVersion(): boolean',
  'get readWithIfModifiedSince(): boolean',
  'get readWithIfUnmodifiedSince(): boolean',
  'get readWithIfMatch(): boolean',
  'get readWithIfNoneMatch(): boolean',
  'get readWithOverrideCacheControl(): boolean',
  'get readWithOverrideContentDisposition(): boolean',
  'get readWithOverrideContentType(): boolean',
  'get write(): boolean',
  'get writeCanMulti(): boolean',
  'get writeCanEmpty(): boolean',
  'get writeCanAppend(): boolean',
  'get writeWithContentType(): boolean',
  'get writeWithContentDisposition(): boolean',
  'get writeWithCacheControl(): boolean',
  'get writeWithContentEncoding(): boolean',
  'get writeWithUserMetadata(): boolean',
  'get writeWithIfMatch(): boolean',
  'get writeWithIfNoneMatch(): boolean',
  'get writeWithIfNotExists(): boolean',
  'get writeMultiMaxSize(): bigint | null',
  'get writeMultiMinSize(): bigint | null',
  'get writeTotalMaxSize(): bigint | null',
  'get createDir(): boolean',
  'get delete(): boolean',
  'get deleteWithVersion(): boolean',
  'get copy(): boolean',
  'get rename(): boolean',
  'get list(): boolean',
  'get listWithLimit(): boolean',
  'get listWithStartAfter(): boolean',
  'get listWithRecursive(): boolean',
  'get listWithVersions(): boolean',
  'get listWithDeleted(): boolean',
  'get presign(): boolean',
  'get presignRead(): boolean',
  'get presignStat(): boolean',
  'get presignWrite(): boolean',
  'get presignDelete(): boolean',
  'get shared(): boolean',
]

function declaredMembers() {
  const dts = readFileSync(path.resolve(__dirname, '../generated.d.ts'), 'utf8')
  const body = dts.match(/^export declare class Capability \{\n([\s\S]*?)^\}/m)
  assert.ok(body, 'generated.d.ts declares class Capability')
  return body[1].split('\n').map((line) => line.trim())
}

function matchesType(value, type) {
  return type.split(' | ').some((t) => {
    switch (t) {
      case 'null':
        return value === null
      case 'boolean':
      case 'bigint':
        return typeof value === t
      default:
        throw new Error(`unknown type ${t}`)
    }
  })
}

describe('Capability keeps its historical surface', () => {
  const cap = new Operator('memory').capability()
  const members = declaredMembers()

  for (const member of FROZEN_MEMBERS) {
    const [, name, type] = member.match(/^get (\w+)\(\): (.+)$/)

    test(member, () => {
      assert.include(members, member)

      const descriptor = Object.getOwnPropertyDescriptor(Capability.prototype, name)
      assert.ok(descriptor, `Capability.prototype.${name} exists`)
      assert.isFunction(descriptor.get, `${name} is a getter`)
      assert.ok(matchesType(cap[name], type), `${name} returned ${String(cap[name])}, expected ${type}`)
    })
  }
})
