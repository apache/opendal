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
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const cheerio = require("cheerio");
const localImages = require("./local-images");

async function fixture(t, baseUrl = "/") {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opendal-images-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  t.mock.method(globalThis, "fetch", () =>
    assert.fail("Image processing must not use the network"),
  );
  const files = {
    "core/Cargo.toml": '[workspace.package]\nversion = "9.1.0"\n',
    "bindings/nodejs/package.json": '{"version":"2.3.4"}',
    "bindings/ruby/Cargo.toml": '[package]\nversion = "4.5.6"\n',
    "website/static/img/architectural.png": "current architecture",
    "website/static/img/logo.svg": '<svg xmlns="http://www.w3.org/2000/svg"/>',
  };
  for (const [name, content] of Object.entries(files)) {
    const filename = path.join(root, name);
    await fs.mkdir(path.dirname(filename), { recursive: true });
    await fs.writeFile(filename, content);
  }
  const siteDir = path.join(root, "website");
  const outDir = path.join(siteDir, "build");
  await fs.mkdir(path.join(outDir, "docs/nodejs"), { recursive: true });
  return { ...localImages({ siteDir, baseUrl }), outDir };
}

for (const baseUrl of ["/", "/opendal/opendal-docs-stable/"]) {
  test(`standalone API images work offline under ${baseUrl}`, async (t) => {
    const { plugin, outDir } = await fixture(t, baseUrl);
    const filename = path.join(outDir, "docs/nodejs/index.html");
    await fs.writeFile(
      filename,
      `
      <a href="https://www.npmjs.com/package/opendal"><img id="version" width="120"
        alt="Latest Version" src="https://img.shields.io/npm/v/opendal.svg?logo=npm"></a>
      <img id="architecture" src="https://opendal.apache.org/img/architectural.png">
      <img id="logo" src="https://opendal.apache.org/img/logo.svg">
      <img id="existing" src="local.svg">
    `,
    );
    await plugin().postBuild({ outDir });
    const $ = cheerio.load(await fs.readFile(filename, "utf8"));
    assert.equal(
      $("#architecture").attr("src"),
      `${baseUrl}img/architectural.png`,
    );
    assert.equal($("#logo").attr("src"), `${baseUrl}img/logo.svg`);
    assert.equal($("#existing").attr("src"), "local.svg");
    assert.equal(
      $("#version").parent().attr("href"),
      "https://www.npmjs.com/package/opendal",
    );
    assert.equal($("#version").attr("width"), "120");
    assert.equal($("#version").attr("alt"), "Node.js docs: v2.3.4");
    const svg = Buffer.from(
      $("#version").attr("src").split(",")[1],
      "base64",
    ).toString();
    assert.match(svg, /aria-label="Node.js docs: v2\.3\.4"/);
    assert.doesNotMatch(svg, /(?:href|src)="https?:/);
    const rewritten = await fs.readFile(filename, "utf8");
    await plugin().postBuild({ outDir });
    assert.equal(await fs.readFile(filename, "utf8"), rewritten);
  });
}

test("Markdown images embed independent documentation versions before client rendering", async (t) => {
  const { rehype } = await fixture(t);
  const images = [
    ["https://img.shields.io/crates/v/opendal.svg", "Rust docs: v9.1.0"],
    [
      "https://img.shields.io/npm/v/opendal.svg?logo=npm",
      "Node.js docs: v2.3.4",
    ],
    ["https://img.shields.io/gem/v/opendal", "Ruby docs: v4.5.6"],
  ];
  const tree = {
    type: "root",
    children: images.map(([src]) => ({
      type: "element",
      tagName: "p",
      children: [{ type: "element", tagName: "img", properties: { src } }],
    })),
  };
  rehype()(tree, { fail: assert.fail });
  tree.children.forEach((node, i) => {
    const { src, alt } = node.children[0].properties;
    assert.match(src, /^data:image\/svg\+xml;base64,/);
    assert.equal(alt, images[i][1]);
  });
});

test("literal MDX img elements use the same source resolution before hydration", async (t) => {
  const { rehype } = await fixture(t, "/preview/");
  const tree = {
    type: "mdxJsxFlowElement",
    name: "img",
    attributes: [
      {
        type: "mdxJsxAttribute",
        name: "src",
        value: "https://opendal.apache.org/img/architectural.png",
      },
      { type: "mdxJsxAttribute", name: "width", value: "100%" },
    ],
  };
  rehype()(tree, { fail: assert.fail });
  assert.equal(tree.attributes[0].value, "/preview/img/architectural.png");
  assert.equal(tree.attributes[1].value, "100%");
});

test("unknown external images identify the affected API page instead of falling back to a network request", async (t) => {
  const { plugin, outDir } = await fixture(t);
  await fs.writeFile(
    path.join(outDir, "docs/nodejs/index.html"),
    '<img src="//example.com/unknown.png">',
  );
  await assert.rejects(
    plugin().postBuild({ outDir }),
    /docs\/nodejs\/index\.html: Unsupported external image: \/\/example\.com\/unknown\.png/,
  );
});
