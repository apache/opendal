const { loadBinding } = require('@node-rs/helper')

module.exports = loadBinding(__dirname, 'opendal', 'opendal')
