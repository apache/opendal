/*
 * Copyright 2022 Datafuse Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import test from 'ava'

import { Operator, Scheme } from '../index.js'

test('test memory write & read', async (t) => {
  let op = new Operator(Scheme.Memory)

  let content = "hello world"
  let path = 'test'

  await op.write(path, content)

  let meta = await op.stat(path)
  t.is(meta.isFile(), true)
  t.is(meta.contentLength, BigInt(content.length))

  let res = await op.read(path)
  t.is(content, new TextDecoder().decode(res))

  await op.delete(path)
})


test('test memory write & read synchronously', (t) => {
  let op = new Operator(Scheme.Memory)

  let content = "hello world"
  let path = 'test'

  op.writeSync(path, content)

  let meta = op.statSync(path)
  t.is(meta.isFile(), true)
  t.is(meta.contentLength, BigInt(content.length))

  let res = op.readSync(path)
  t.is(content, new TextDecoder().decode(res))

  op.deleteSync(path)
})

test('test scan', async (t) => {
  let op = new Operator(Scheme.Memory)
  let content = "hello world"
  let pathPrefix = 'test'
  let paths = new Array(10).fill(0).map((_, index) => pathPrefix + index)

  let writeTasks = paths.map((path) => new Promise<void>(async (resolve) => {
    await op.write(path, Buffer.from(content))
    resolve()
  }))

  await Promise.all(writeTasks)

  let objList = await op.scan("")
  let entryCount = 0
  while (true) {
    let entry = await objList.next()
    if (entry === null) break

    entryCount++
    t.is(new TextDecoder().decode(await op.read(entry.path())), content)
  }

  t.is(entryCount, paths.length)

  paths.forEach(async (path) => {
    await op.delete(path)
  })
})


test('test scan sync',async (t) => {
  let op = new Operator(Scheme.Memory)
  let content = "hello world"
  let pathPrefix = 'test'
  let paths = new Array(10).fill(0).map((_, index) => pathPrefix + index)

  let writeTasks = paths.map((path) => new Promise<void>(async (resolve) => {
    await op.write(path, Buffer.from(content))
    resolve()
  }))

  await Promise.all(writeTasks)

  let objList = op.scanSync("")
  let entryCount = 0
  while (true) {
    let entry = objList.next()
    if (entry === null) break

    entryCount++
    t.is(new TextDecoder().decode(await op.read(entry.path())), content)
  }

  t.is(entryCount, paths.length)

  paths.forEach(async (path) => {
    await op.delete(path)
  })
})


