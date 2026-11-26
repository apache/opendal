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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { execFile } from 'node:child_process'
import { once } from 'node:events'
import { createServer } from 'node:http'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { expect, test } from 'vitest'

const runChild = promisify(execFile)
const packageDirectory = fileURLToPath(new URL('../', import.meta.url))

async function startHeadServer(onTestFinished, length, { retryFirst = false } = {}) {
  const requests = []
  const server = createServer((request, response) => {
    requests.push([request.method, request.url])
    const status = retryFirst && requests.length === 1 ? 503 : 200
    response.writeHead(status, { 'Content-Length': status === 200 ? length : 0 }).end()
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  onTestFinished(() => new Promise((resolve) => server.close(resolve)))
  return { url: `http://127.0.0.1:${server.address().port}`, requests }
}

const client = `
  const assert = require('node:assert/strict')
  const { Operator } = require('opendal')
  const [constructor, stat, origin, proxy] = process.argv.slice(1)

  const service = {
    endpoint: origin,
    region: 'us-west-2',
    skip_signature: 'true',
    disable_config_load: 'true',
    disable_ec2_metadata: 'true',
  }
  function open(options) {
    if (constructor === 'map') {
      return new Operator('s3', { bucket: 'proxy-test', ...options })
    }
    return Operator.fromUri('s3://' + 'proxy-test?' + new URLSearchParams(options))
  }

  const proxied = open({
    ...service,
    'context.http.proxy': proxy,
    layers: 'retry',
    'layer.retry.min_delay': '1ms',
  })
  const direct = open(service)
  ;(async () => {
    assert.equal((await direct[stat]('direct-before')).contentLength, 3n)
    assert.equal((await proxied[stat]('proxied')).contentLength, 7n)
    assert.equal((await direct[stat]('direct-after')).contentLength, 3n)
  })().catch((error) => {
    console.error(error)
    process.exitCode = 1
  })
`

test.for([
  ['map', 'stat'],
  ['map', 'statSync'],
  ['uri', 'stat'],
  ['uri', 'statSync'],
])('Operator %s uses a per-operator proxy and retries the proxied %s request', async ([constructor, stat], { onTestFinished }) => {
  const origin = await startHeadServer(onTestFinished, 3)
  const proxy = await startHeadServer(onTestFinished, 7, { retryFirst: true })
  const proxyVariables = new Set(['http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'])
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !proxyVariables.has(key.toLowerCase())))

  // Async process launch lets the parent serve both endpoints during statSync.
  await runChild(process.execPath, ['-e', client, constructor, stat, origin.url, proxy.url], {
    cwd: packageDirectory,
    env,
    timeout: 20_000,
  })

  expect(origin.requests).toEqual([
    ['HEAD', '/proxy-test/direct-before'],
    ['HEAD', '/proxy-test/direct-after'],
  ])
  expect(proxy.requests).toEqual([
    ['HEAD', `${origin.url}/proxy-test/proxied`],
    ['HEAD', `${origin.url}/proxy-test/proxied`],
  ])
})
