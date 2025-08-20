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

import { Operator } from '../index.mjs'
import 'jsr:@std/dotenv/load'
import { S3Client } from 'jsr:@bradenmacdonald/s3-lite-client'

const endpoint = Deno.env.get('AWS_S3_ENDPOINT') || 'http://localhost:9000'
const url = new URL(endpoint)
const port = url.port ? parseInt(url.port) : 9000
const hostname = url.hostname
const bucket = Deno.env.get('AWS_BUCKET') || 'benchmark'

const s3 = new S3Client({
  endPoint: hostname,
  useSSL: false,
  port: port,
  region: Deno.env.get('AWS_REGION') || 'us-east-1',
  bucket,
  accessKey: Deno.env.get('AWS_ACCESS_KEY_ID'),
  secretKey: Deno.env.get('AWS_SECRET_ACCESS_KEY'),
})

const opendal = new Operator('s3', {
  root: '/',
  bucket,
  endpoint,
})

const files = [
  {
    name: '4kb',
    file: new Uint8Array(4 * 1024),
  },
  {
    name: '256kb',
    file: new Uint8Array(256 * 1024),
  },
  {
    name: '4mb',
    file: new Uint8Array(4 * 1024 * 1024),
  },
  {
    name: '16mb',
    file: new Uint8Array(16 * 1024 * 1024),
  },
]

const textDecoder = new TextDecoder()
const filenames = await Promise.all(
  files.map(async (data) => {
    const filename = `${crypto.randomUUID()}-${data.name}-read-bench`
    await s3.putObject(filename, textDecoder.decode(data.file))
    return filename
  }),
)
files.map((data, i) => {
  Deno.bench(
    `opendal: read ${data.name}`,
    {
      group: `read ${data.name}`,
    },
    async () => {
      await opendal.read(filenames[i])
    },
  )
  Deno.bench(
    `s3: read ${data.name}`,
    {
      group: `read ${data.name}`,
    },
    async () => {
      await s3.getObject(filenames[i])
    },
  )
})

files.map((data) => {
  Deno.bench(
    `s3: write ${data.name}`,
    {
      group: `write ${data.name}`,
    },
    async () => {
      const filename = `${crypto.randomUUID()}-${data.name}}-s3`
      await s3.putObject(filename, textDecoder.decode(data.file))
    },
  )
  Deno.bench(
    `opendal: write ${data.name}`,
    {
      group: `write ${data.name}`,
    },
    async () => {
      const filename = `${crypto.randomUUID()}-${data.name}}-opendal`
      await opendal.write(filename, textDecoder.decode(data.file))
    },
  )
})
