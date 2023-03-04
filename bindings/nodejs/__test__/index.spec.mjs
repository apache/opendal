// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


import test from 'ava'

import { Memory } from '../index.js'

test('test memory write & read', async (t) => {
  let builder = new Memory()
  let op = builder.build()
  let content = "hello world"
  let path = 'test'

  let o = op.object(path)

  await o.write(new TextEncoder().encode(content))

  let meta = await o.stat()
  t.is(meta.mode, 0)
  t.is(meta.contentLength, content.length)

  let res = await o.read()
  t.is(content, new TextDecoder().decode(res))

  await o.delete()
})


test('test memory write & read synchronously', (t) => {
  let builder = new Memory()
  let op = builder.build()
  let content = "hello world"
  let path = 'test'

  let o = op.object(path)

  o.writeSync(new TextEncoder().encode(content))

  let meta = o.statSync()
  t.is(meta.mode, 0)
  t.is(meta.contentLength, content.length)

  let res = o.readSync()
  t.is(content, new TextDecoder().decode(res))

  o.deleteSync()
})

test('scan', async (t) => {
  let builder = new Memory()
  let op = builder.build()
  let content = "hello world"
  let path = 'test'

  let o = op.object(path)
  let o2 = op.object(path + '1')
  o.write(new TextEncoder().encode(content))
  o2.write(new TextEncoder().encode(content))

  let dir = op.object("")
  let objList = await dir.scan()
  let currentPage = await objList.nextPage()
  for (let page of currentPage) {
    t.is(new TextDecoder().decode(await page.read()), content)
  }

  await o.delete()
  await o2.delete()
})
