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

const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");

test("Rustdoc attributes do not affect public documentation extraction", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "opendal-rustdoc-json-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const input = path.join(dir, "rustdoc.json");
  const output = path.join(dir, "llms.json");
  const publicItem = {
    name: "opendal",
    visibility: "public",
    docs: "Public Rust documentation.",
    span: { filename: "src/lib.rs" },
    attrs: [{ other: "#[doc = \"Public Rust documentation.\"]" }],
  };
  fs.writeFileSync(input, JSON.stringify({
    root: 0,
    index: {
      0: publicItem,
      1: { ...publicItem, visibility: "default", docs: "Private details." },
      2: { ...publicItem, span: null, docs: "Generated without a source page." },
    },
  }));

  execFileSync(process.execPath, [
    path.join(__dirname, "generate-rustdoc-llms.cjs"), input, output,
  ]);

  const result = JSON.parse(fs.readFileSync(output, "utf8"));
  assert.equal(result.sessions.length, 2);
  assert.equal(result.sessions[0].link, "opendal/");
  assert.deepEqual(result.fullSessions, [{
    content: publicItem.docs,
    link: "src/opendal/lib.rs.html",
  }]);
});
