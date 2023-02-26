import test from 'ava'

import { debug } from '../index.js'

test('sum from native', (t) => {
  t.is(debug(), "test")
})
