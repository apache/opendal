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
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const { addRustdocLlmSessions } = require("./site-helpers");

function artifact(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "opendal-rustdoc-llms-"));
  const filename = path.join(dir, "llms.json");
  const previous = process.env.OPENDAL_RUSTDOC_LLMS;
  process.env.OPENDAL_RUSTDOC_LLMS = filename;
  t.after(() => {
    if (previous === undefined) delete process.env.OPENDAL_RUSTDOC_LLMS;
    else process.env.OPENDAL_RUSTDOC_LLMS = previous;
    fs.rmSync(dir, { recursive: true, force: true });
  });
  return filename;
}

for (const baseUrl of ["/", "/opendal/opendal-docs-stable/"]) {
  test(`Rust artifacts supplement website content under ${baseUrl}`, (t) => {
    const filename = artifact(t);
    fs.writeFileSync(filename, JSON.stringify({
      libName: "opendal",
      sessions: [
        {
          title: "opendal",
          description: "Rust API",
          link: "opendal/",
        },
        {
          title: "init_default_registry",
          description: "Initialize the registry",
          link: "src/opendal/lib.rs.html",
        },
      ],
      fullSessions: [{
        content: "Initialize the default registry.",
        link: "src/opendal/lib.rs.html",
      }],
    }));
    const websiteIndex = { sessionName: "Docs", items: [] };
    const websiteFull = {
      content: "Website guide",
      link: "https://opendal.apache.org/docs/",
    };
    const ctx = {
      llmConfig: {
        llmStdConfig: { sessions: [websiteIndex] },
        llmFullStdConfig: { sessions: [websiteFull] },
      },
    };

    addRustdocLlmSessions(ctx, baseUrl);

    const [rust, docs] = ctx.llmConfig.llmStdConfig.sessions;
    const [rustFull, guide] = ctx.llmConfig.llmFullStdConfig.sessions;
    const root = `https://opendal.apache.org${baseUrl}docs/rust/`;
    assert.equal(rust.items[0].link, `${root}opendal/`);
    assert.equal(rust.items[1].link, `${root}src/opendal/lib.rs.html`);
    assert.equal(rustFull.link, `${root}src/opendal/lib.rs.html`);
    assert.equal(rustFull.content, "Initialize the default registry.");
    assert.equal(docs, websiteIndex);
    assert.equal(guide, websiteFull);
  });
}

test("configured Rust artifacts cannot silently disappear or be empty", (t) => {
  const filename = artifact(t);
  assert.throws(() => addRustdocLlmSessions({}), { code: "ENOENT" });
  fs.writeFileSync(filename, "null");
  assert.throws(() => addRustdocLlmSessions({}), /Rust LLM documentation is empty/);
});
