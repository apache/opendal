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

import { describe } from 'vitest'
import { Operator } from '../../index.js'
import { loadConfigFromEnv } from '../utils.mjs'

import { run as AsyncIOTestRun } from './async.suite.mjs'
import { run as MetaTestRun } from './meta.suite.mjs'
import { run as SyncIOTestRun } from './sync.suite.mjs'

export function runner(testName, scheme) {
    const config = loadConfigFromEnv(scheme)
    const operator = scheme ? new Operator(scheme, config) : undefined

    describe.skipIf(!operator)(testName, () => {
        AsyncIOTestRun(operator)
        MetaTestRun(operator)
        SyncIOTestRun(operator)
    })
}
