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

const assert = require('assert')
const { Operator } = require('../index.js')
const { Given, When, Then } = require('@cucumber/cucumber')

Given('A new OpenDAL Blocking Operator', function () {
    this.op = new Operator('memory')
})

When('Blocking write path {string} with content {string}', function (path, content) {
    this.op.writeSync(path, content)
})

Then('The blocking file {string} should exist', function (path) {
    this.op.statSync(path)
})

Then('The blocking file {string} entry mode must be file', function (path) {
    let meta = this.op.statSync(path)
    assert(meta.isFile())
})

Then('The blocking file {string} content length must be {int}', function (path, size) {
    let meta = this.op.statSync(path)
    assert(meta.contentLength == size)
})

Then('The blocking file {string} must have content {string}', function (path, content) {
    let bs = this.op.readSync(path)
    assert(bs.toString() == content)
})

Given('A new OpenDAL Async Operator', function () {
    this.op = new Operator('memory')
})

When('Async write path {string} with content {string}', async function (path, content) {
    await this.op.write(path, content)
})

Then('The async file {string} should exist', async function (path) {
    await this.op.stat(path)
})

Then('The async file {string} entry mode must be file', async function (path) {
    let meta = await this.op.stat(path)
    assert(meta.isFile())
})

Then('The async file {string} content length must be {int}', async function (path, size) {
    let meta = await this.op.stat(path)
    assert(meta.contentLength == size)
})

Then('The async file {string} must have content {string}', async function (path, content) {
    let bs = await this.op.read(path)
    assert(bs.toString() == content)
})
