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

const fs = require("node:fs");
const path = require("node:path");
const { performance } = require("node:perf_hooks");
const { fromLocal } = require("crates-llms-txt");

const [manifestPath, toolchain, output] = process.argv.slice(2);
if (!manifestPath || !toolchain || !output) {
  throw new Error(
    "Usage: generate-rustdoc-llms.cjs <Cargo.toml> <toolchain> <output.json>",
  );
}

console.log(
  `Generating Rust LLM documentation with ${toolchain}: ${manifestPath}`,
);
const started = performance.now();
// Use the docs job's toolchain and existing Cargo target directory.
// The converter also runs Cargo metadata, which can fetch non-host dependencies.
const config = fromLocal(path.resolve(manifestPath), toolchain);
if (!config?.sessions?.length || !config?.fullSessions?.length) {
  throw new Error(
    `Failed to generate Rust LLM documentation from ${manifestPath} with ${toolchain}`,
  );
}

fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, JSON.stringify(config));
console.log(
  `Generated ${config.sessions.length} index entries and ${config.fullSessions.length} full-text entries in ${((performance.now() - started) / 1000).toFixed(2)} s: ${output}`,
);
