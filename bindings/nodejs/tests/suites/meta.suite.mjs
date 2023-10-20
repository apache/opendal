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

import { randomUUID } from 'node:crypto'
import { expect, test } from 'vitest'
import { generateBytes } from '../utils.mjs'

export function run(operator) {
    test('meta info properties tests', async () => {
        const filename = `random_file_${randomUUID()}`
        const content = generateBytes()

        await operator.write(filename, content)

        const meta = await operator.stat(filename)
        expect(meta.isFile()).toBeTruthy()
        expect(meta.contentLength).toEqual(BigInt(content.length))
        expect(meta.contentDisposition).toBeNull()
        expect(meta.contentMd5).toBeNull()
        expect(meta.contentType).toBeNull()
        expect(meta.etag).toBeNull()

        await operator.delete(filename)
    })
}
