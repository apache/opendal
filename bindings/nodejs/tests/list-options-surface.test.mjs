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

import { Operator } from '../index.mjs'

// `ListOptions` and the list methods as `generated.d.ts` declared them before
// `ListOptions` was generated from core. Callers pass these options as object
// literals or implement the interface structurally, so each member must stay
// with its type and stay optional.
const FROZEN_MEMBERS = [
  'limit?: number',
  'startAfter?: string',
  'recursive?: boolean',
  'versions?: boolean',
  'deleted?: boolean',
]

const FROZEN_SIGNATURES = [
  'list(path: string, options?: ListOptions | undefined | null): Promise<Array<Entry>>',
  'listSync(path: string, options?: ListOptions | undefined | null): Array<Entry>',
  'lister(path: string, options?: ListOptions | undefined | null): Promise<Lister>',
  'listerSync(path: string, options?: ListOptions | undefined | null): BlockingLister',
]

const SAMPLES = { number: 0, string: 'a', boolean: false }

function declared(kind, name) {
  const dts = readFileSync(path.resolve(__dirname, '../generated.d.ts'), 'utf8')
  const body = dts.match(new RegExp(`^export ${kind} ${name} \\{\\n([\\s\\S]*?)^\\}`, 'm'))
  assert.ok(body, `generated.d.ts declares ${kind} ${name}`)
  return body[1]
    .split('\n')
    .filter((line) => /^ {2}\w/.test(line))
    .map((line) => line.trim())
}

describe('ListOptions keeps its historical surface', () => {
  const op = new Operator('memory')
  const members = declared('interface', 'ListOptions')

  test('every member is optional', () => {
    for (const member of members) {
      assert.match(member, /^\w+\?: /)
    }
  })

  for (const signature of FROZEN_SIGNATURES) {
    test(signature, () => {
      assert.include(declared('declare class', 'Operator'), signature)
    })
  }

  for (const member of FROZEN_MEMBERS) {
    const [, field, type] = member.match(/^(\w+)\?: (.+)$/)

    test(member, async () => {
      assert.include(members, member)

      // Each list method must accept the declared type instead of failing
      // with a napi `*Expected` error.
      const options = { [field]: SAMPLES[type] }
      for (const list of [
        () => op.list('/', options),
        () => op.listSync('/', options),
        () => op.lister('/', options),
        () => op.listerSync('/', options),
      ]) {
        try {
          await list()
        } catch (err) {
          assert.notMatch(String(err.code), /Expected$/, `${field}: ${err.message}`)
        }
      }
    })
  }
})
