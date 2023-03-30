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

const { suite, add, cycle, complete } = require('benny')
const opendalWrite = require('./opendal.js').write
const s3Write = require('./s3.js').write
const { testFiles } = require('./constant.js')
const crypto = require('node:crypto')

async function bench() {
  const uuid = crypto.randomUUID()
  await testFiles
    .map(
      (v) => () =>
        suite(
          `write (${v.name})`,
          add(`opendal write (${v.name})`, async () => {
            let count = 0
            return async () => opendalWrite(`${uuid}_${count++}_${v.name}_opendal.txt`, v.file)
          }),
          add(`s3 write (${v.name})`, async () => {
            let count = 0
            return async () => s3Write(`${uuid}_${count++}_${v.name}_s3.txt`, v.file)
          }),
          cycle(),
          complete(),
        ),
    )
    .reduce((p, v) => p.then(() => v()), Promise.resolve())
}

module.exports = bench
