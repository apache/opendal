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

const endpoint = process.env.AWS_S3_ENDPOINT
const region = process.env.AWS_S3_REGION
const accessKeyId = process.env.AWS_ACCESS_KEY_ID
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY
const bucket = 'benchmark'

const file_4kb = Buffer.alloc(4 * 1024, 'opendal', 'utf8')
const file_256kb = Buffer.alloc(256 * 1024, 'opendal', 'utf8')
const file_4mb = Buffer.alloc(4 * 1024 * 1024, 'opendal', 'utf8')
const file_16mb = Buffer.alloc(16 * 1024 * 1024, 'opendal', 'utf8')

const testFiles = [
  { name: '4kb', file: file_4kb },
  { name: '256kb', file: file_256kb },
  { name: '4mb', file: file_4mb },
  { name: '16mb', file: file_16mb },
]

module.exports.endpoint = endpoint
module.exports.region = region
module.exports.accessKeyId = accessKeyId
module.exports.secretAccessKey = secretAccessKey
module.exports.bucket = bucket
module.exports.testFiles = testFiles