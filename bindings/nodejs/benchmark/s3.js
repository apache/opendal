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

const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3')
const { endpoint, region, accessKeyId, secretAccessKey, bucket } = require('./constant.js')

const client = new S3Client({
  endpoint,
  region,
  credentials: {
    accessKeyId,
    secretAccessKey,
  },
})

const write = (path, data) => {
  const command = new PutObjectCommand({
    Body: data,
    Key: path,
    Bucket: bucket,
  })
  return client.send(command)
}

const read = (path) => {
  const command = new GetObjectCommand({
    Key: path,
    Bucket: bucket,
  })
  return client.send(command)
}

module.exports.read = read
module.exports.write = write
