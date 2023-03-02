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

import { OperatorFactory } from '../index.js'

test('test memory write & read', async (t) => {
  let op = OperatorFactory.memory()
  let content = "hello wowld"
  let path = 'test.txt'

  await op.put(path, content)

  let meta = await op.head(path)
  t.is(meta.size, content.length)

  let res = await op.get('test.txt')
  t.is(content, res)

  await op.delete(path)
})
