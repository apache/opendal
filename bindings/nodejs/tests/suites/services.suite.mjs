export function run(operator) {
  test('get capability', () => {
    assert.ok(operator.capability())
    assert.ok(operator.capability().read)
  })

  test('try to non-exist capability', () => {
    assert.ok(operator.capability())
    assert.ifError(operator.capability().nonExist, 'try get a non-exist capability should return undefined')
  })
}
