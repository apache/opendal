const read = require('./read.js')
const write = require('./write.js')

async function bench() {
  await write()
  await read()
}
bench()
