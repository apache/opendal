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
const { s3, opendal, testFiles } = require('./lib.js')
const crypto = require('node:crypto')

async function bench() {
  const uuid = crypto.randomUUID()
  await testFiles
    .map((v) => async () => {
      const filename = `${uuid}_${v.name}_read_bench.txt`
      await opendal.write(filename, v.file)
      return suite(
        `read (${v.name})`,
        add(`s3 read (${v.name})`, async () => {
          await s3.read(filename).then((v) => v.Body.transformToString('utf-8'))
        }),
        add(`opendal read (${v.name})`, async () => {
          await opendal.read(filename).then((v) => v.toString('utf-8'))
        }),
        cycle(),
        complete(),
      )
    })
    .reduce((p, v) => p.then(() => v()), Promise.resolve())
}

module.exports = bench
