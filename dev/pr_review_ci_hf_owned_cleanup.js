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

// Task-only cleanup of fixtures created by this task's two failed HF jobs.
const assert = require("node:assert/strict");
const { hfRequest } = require("../.github/actions/hf-temp-repo/common");

const datasets = [
  "opendal/test-dataset-41130652274-test-a13d49f9",
  "opendal/test-dataset-41130652274-test-0e0e99db",
];

async function main() {
  const token = process.env.OPENDAL_HF_TOKEN;
  assert(token, "OPENDAL_HF_TOKEN is required");
  await hfRequest("GET", "/api/whoami-v2", token);

  for (const id of datasets) {
    let metadata;
    try {
      metadata = JSON.parse(await hfRequest("GET", `/api/datasets/${id}`, token));
    } catch (error) {
      if (!/^HTTP 404:/.test(error.message)) throw error;
      console.log(`${id}: metadata returned 404; no DELETE sent`);
      continue;
    }
    assert.equal(metadata.id, id, "HF metadata must match the exact task fixture");
    const [organization, name] = id.split("/");
    await hfRequest("DELETE", "/api/repos/delete", token, {
      type: "dataset",
      organization,
      name,
    });
    try {
      await hfRequest("GET", `/api/datasets/${id}`, token);
      throw new Error(`${id}: metadata still exists after DELETE`);
    } catch (error) {
      if (!/^HTTP 404:/.test(error.message)) throw error;
    }
    console.log(`${id}: exact metadata matched; DELETE succeeded; GET returned 404`);
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
