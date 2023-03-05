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
import { Operator, Fs } from '../index.js'
import fs from "node:fs"

const testFolderPath = "./testData"
const fileContent = "hello world"
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
test.before((t) => {
  fs.mkdir(testFolderPath, () => {})
})

test.after((t) => {
  fs.rmdir(testFolderPath, () => {})
})

test("test fs read & write", async (t) => {
  let op = new Operator("fs", {
    root: testFolderPath
  })

  let o = op.object(t.title)
  await o.write(textEncoder.encode(fileContent))

  t.is(textDecoder.decode(await o.read()), fileContent)

  await o.delete()
})

test("test fs read & write 2", async (t) => {
  let op = new Fs({
    root: testFolderPath
  })

  let o = op.object(t.title)
  await o.write(textEncoder.encode(fileContent))

  t.is(textDecoder.decode(await o.read()), fileContent)

  await o.delete()
})
