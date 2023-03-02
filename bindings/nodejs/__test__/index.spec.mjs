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
