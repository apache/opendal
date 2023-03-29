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

const http = require('node:http')
const url = require('node:url')
const { Operator } = require('../index')

const op = new Operator('s3', {
  root: '/',
  bucket: 'example-bucket',
})

const server = http.createServer(async (req, res) => {
  res.setHeader('Content-Type', 'text/json; charset=utf-8')

  if (req.url.startsWith('/presign') && req.method === 'GET') {
    const urlParts = url.parse(req.url, true)
    const path = urlParts.query.path
    const expires = urlParts.query.expires

    const presignedRequest = op.presignRead(path, parseInt(expires))

    res.statusCode = 200
    res.end(JSON.stringify(presignedRequest))
  } else {
    res.statusCode = 404
    res.end('Not Found')
  }
})

server.listen(3000, () => {
  console.log('Server is listening on port 3000.')
})
