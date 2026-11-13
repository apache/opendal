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

// Content model for the landing page. All copy and catalog data lives here so
// the section components stay presentational. Code samples are taken from the
// real binding APIs so the page never drifts from the libraries it advertises.

export const REPO_URL = "https://github.com/apache/opendal";
export const DOCS_URL = "/docs/";
export const DISCORD_URL = "https://discord.gg/XQy8yGR2dg";

export const heroStats = [
  { value: "50+", label: "services" },
  { value: "17", label: "languages" },
  { value: "5k+", label: "stars" },
  { value: "10M+", label: "downloads" },
];

// One operator model across ecosystems. Each binding follows its own API style.
export const codeSamples = [
  {
    id: "rust",
    label: "Rust",
    language: "rust",
    install: "$ cargo add [opendal](/docs/core)",
    code: `use opendal::services::S3;
use opendal::Operator;

// Configure a backend once, then use one operator.
let builder = S3::default().bucket("data");
let op = Operator::new(builder)?;

op.write("hello.txt", "Hello, World!").await?;
let bytes = op.read("hello.txt").await?;`,
  },
  {
    id: "java",
    label: "Java",
    language: "java",
    install: "[org.apache.opendal:opendal-java](/docs/bindings/java)",
    code: `import org.apache.opendal.AsyncOperator;
import java.util.Map;

// Configure a backend once, then use one operator.
var config = Map.of("bucket", "data");
try (var op = AsyncOperator.of("s3", config)) {
    op.write("hello.txt", "Hello, World!").join();
    byte[] data = op.read("hello.txt").join();
}`,
  },
  {
    id: "python",
    label: "Python",
    language: "python",
    install: "$ pip install [opendal](/docs/bindings/python)",
    code: `import opendal

# Configure a backend once, then use one operator.
op = opendal.Operator("s3", bucket="data")

op.write("hello.txt", b"Hello, World!")
data = op.read("hello.txt")`,
  },
  {
    id: "node",
    label: "Node.js",
    language: "javascript",
    install: "$ npm install [opendal](/docs/bindings/nodejs)",
    code: `import { Operator } from "opendal";

// Configure a backend once, then use one operator.
const op = new Operator("s3", { bucket: "data" });

await op.write("hello.txt", "Hello, World!");
const data = await op.read("hello.txt");`,
  },
  {
    id: "ruby",
    label: "Ruby",
    language: "ruby",
    install: "$ gem install [opendal](/docs/bindings/ruby)",
    code: `require "opendal"

# Configure a backend once, then use one operator.
op = OpenDAL::Operator.new("s3", { "bucket" => "data" })

op.write("hello.txt", "Hello, World!")
data = op.read("hello.txt")`,
  },
  {
    id: "go",
    label: "Go",
    language: "go",
    install: "$ go get [github.com/apache/opendal/bindings/go](/docs/bindings/go)",
    code: `import (
    "github.com/apache/opendal-go-services/s3"
    opendal "github.com/apache/opendal/bindings/go"
)

// Configure a backend once, then use one operator.
opts := opendal.OperatorOptions{"bucket": "data"}
op, _ := opendal.NewOperator(s3.Scheme, opts)
defer op.Close()

op.Write("hello.txt", []byte("Hello, World!"))
data, _ := op.Read("hello.txt")`,
  },
  {
    id: "c",
    label: "C",
    language: "c",
    install: "[opendal-c](/docs/bindings/c)",
    code: `#include <opendal.h>

// Configure a backend once, then use one operator.
opendal_operator_options *opts = opendal_operator_options_new();
opendal_operator_options_set(opts, "bucket", "data");
opendal_operator *op = NULL;
opendal_error *err = opendal_operator_new("s3", opts, &op);
opendal_operator_options_free(opts);

opendal_bytes data = { .data = (uint8_t *)"Hello, World!", .len = 13 };
err = opendal_operator_write(op, "hello.txt", &data);

opendal_bytes read = { 0 };
err = opendal_operator_read(op, "hello.txt", &read);`,
  },
  {
    id: "cpp",
    label: "C++",
    language: "cpp",
    install: "[opendal-cpp](/docs/bindings/cpp)",
    code: `#include <opendal.hpp>

// Configure a backend once, then use one operator.
opendal::Operator op("s3", {{"bucket", "data"}});

std::string content = "Hello, World!";
op.Write("hello.txt", content);

std::string data = op.Read("hello.txt");`,
  },
];

export const valueProps = [
  {
    index: "01",
    title: "Ergonomic for a single backend",
    body: "Read, write, stat, list, copy, and delete through one Operator, even when your application only uses S3 or a local directory. Paths, metadata, and error kinds follow one model across operations.",
  },
  {
    index: "02",
    title: "Built for demanding I/O",
    body: "Stream large objects, read only the byte ranges you need, and upload parts concurrently on services with multipart or block writes. Add retries, timeouts, and concurrency limits as layers.",
  },
  {
    index: "03",
    title: "Portable across 50+ services",
    body: "Use memory in tests, the local file system in development, and S3, GCS, Azure Blob, HDFS, or a database in production. Capability checks show where services differ.",
  },
];

// Capability examples for the explorer. Snippets use the Rust core API; the
// section copy points readers to each binding's guide for its own options.
const RS = "https://docs.rs/opendal/latest/opendal";
const opDoc = (method) => `${RS}/struct.Operator.html#method.${method}`;

export const capabilityThemes = [
  {
    title: "Read only what you need",
    blurb: "Fetch a byte range, or a large object in concurrent chunks.",
    doc: opDoc("read_with"),
    code: `let op = Operator::new(S3::default().bucket("data"))?;

// Read just the bytes you need.
let head = op.read_with("logs/today").range(0..64 * 1024).await?;

// Or read a large object in concurrent chunks.
let object = op
    .read_with("big.parquet")
    .concurrent(8)
    .chunk(8 * 1024 * 1024)
    .await?;`,
  },
  {
    title: "Stream large writes",
    blurb: "Write in parts; multipart and block services upload them concurrently.",
    doc: opDoc("writer_with"),
    code: `let op = Operator::new(S3::default().bucket("data"))?;

// Keep up to 8 part uploads in flight.
let mut writer = op
    .writer_with("big.bin")
    .concurrent(8)
    .chunk(8 * 1024 * 1024)
    .await?;

// Write buffers as they arrive. Always check close:
// it waits for in-flight parts and commits the object.
writer.write(part_one).await?;
writer.write(part_two).await?;
writer.close().await?;`,
  },
  {
    title: "Work with files",
    blurb: "Inspect, list, copy, delete, and share files.",
    doc: opDoc("lister_with"),
    code: `// Inspect a file without downloading it.
let meta = op.stat("report.csv").await?;
// List a prefix, recursing lazily through the tree.
let mut entries = op.lister_with("logs/").recursive(true).await?;
while let Some(entry) = entries.try_next().await? {
    println!("{}", entry.path());
}
// Copy within the service, where supported.
op.copy("draft.md", "final.md").await?;
// Recursively delete a subtree.
op.delete_with("tmp/").recursive(true).await?;
// Presign a temporary read request, where supported.
let ttl = Duration::from_secs(3600);
let req = op.presign_read("report.csv", ttl).await?;`,
  },
  {
    title: "Add production policies",
    blurb: "Compose timeouts, retries, and limits; guard writes with conditions.",
    doc: `${RS}/layers/index.html`,
    code: `use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};

// Add TimeoutLayer before RetryLayer so each
// attempt gets its own deadline.
let op = op
    .layer(TimeoutLayer::new().with_timeout(Duration::from_secs(10)))
    .layer(RetryLayer::new().with_max_times(5).with_jitter())
    .layer(ConcurrentLimitLayer::new(64));

// Create only if absent, where supported.
op.write_with("once.json", data).if_not_exists(true).await?;
// Read only if unchanged, where supported.
let doc = op.read_with("doc.json").if_match(&etag).await?;`,
  },
];

// Long-form adoption story shown next to the adopter logos.
export const featuredCaseStudy = {
  title: "How ScopeDB accesses object storage with OpenDAL",
  body: "A columnar database on object storage uses one operator for sparse range reads, concurrent multipart writes, cloud authentication, and separate timeout and retry policies for reads and writes.",
  href: "/blog/2026/11/11/scopedb-on-opendal/",
};

// Real adopters from the project's users lists (core + bindings), ordered by
// GitHub star count (highest first). Logos are the projects' GitHub org
// avatars, self-hosted under static/img/users/. Refresh with scripts as the
// ecosystem grows. The logo wall shows a responsive subset by viewport width.
// Public projects with 1,000+ GitHub stars that depend on OpenDAL, sorted by
// stars. Sourced via crates.io reverse deps + GitHub code search; logos are the
// owner avatars, mirrored locally under static/img/users to avoid runtime calls.
export const usedBy = [
  { name: "Dify", icon: "/img/users/dify.png", href: "https://github.com/langgenius/dify" },
  { name: "RAGFlow", icon: "/img/users/ragflow.png", href: "https://github.com/infiniflow/ragflow" },
  { name: "Pathway", icon: "/img/users/pathway.png", href: "https://github.com/pathwaycom/pathway" },
  { name: "Vaultwarden", icon: "/img/users/vaultwarden.png", href: "https://github.com/dani-garcia/vaultwarden" },
  { name: "LlamaIndex", icon: "/img/users/llamaindex.png", href: "https://github.com/run-llama/llama_index" },
  { name: "Hasura", icon: "/img/users/hasura.png", href: "https://github.com/hasura/graphql-engine" },
  { name: "Vector", icon: "/img/users/vector.png", href: "https://github.com/vectordotdev/vector" },
  { name: "QuestDB", icon: "/img/users/questdb.png", href: "https://github.com/questdb/questdb" },
  { name: "WrenAI", icon: "/img/users/wrenai.png", href: "https://github.com/Canner/WrenAI" },
  { name: "Quickwit", icon: "/img/users/quickwit.png", href: "https://github.com/quickwit-oss/quickwit" },
  { name: "SeaTunnel", icon: "/img/users/seatunnel.png", href: "https://github.com/apache/seatunnel" },
  { name: "Databend", icon: "/img/users/databend.png", href: "https://github.com/databendlabs/databend" },
  { name: "RisingWave", icon: "/img/users/risingwave.png", href: "https://github.com/risingwavelabs/risingwave" },
  { name: "Loco", icon: "/img/users/loco.png", href: "https://github.com/loco-rs/loco" },
  { name: "sccache", icon: "/img/users/sccache.png", href: "https://github.com/mozilla/sccache" },
  { name: "Lance", icon: "/img/users/lance.png", href: "https://github.com/lance-format/lance" },
  { name: "GreptimeDB", icon: "/img/users/greptimedb.png", href: "https://github.com/GreptimeTeam/greptimedb" },
  { name: "Daft", icon: "/img/users/daft.png", href: "https://github.com/Eventual-Inc/Daft" },
  { name: "CrateDB", icon: "/img/users/cratedb.png", href: "https://github.com/crate/crate" },
  { name: "Pants", icon: "/img/users/pants.png", href: "https://github.com/pantsbuild/pants" },
  { name: "rustic", icon: "/img/users/rustic.png", href: "https://github.com/rustic-rs/rustic" },
  { name: "SlateDB", icon: "/img/users/slatedb.png", href: "https://github.com/slatedb/slatedb" },
  { name: "Gravitino", icon: "/img/users/gravitino.png", href: "https://github.com/apache/gravitino" },
  { name: "Spice.ai", icon: "/img/users/spiceai.png", href: "https://github.com/spiceai/spiceai" },
  { name: "Kubeflow Trainer", icon: "/img/users/kubeflow-trainer.png", href: "https://github.com/kubeflow/trainer" },
  { name: "OctoBase", icon: "/img/users/octobase.png", href: "https://github.com/toeverything/OctoBase" },
  { name: "Openraft", icon: "/img/users/openraft.png", href: "https://github.com/databendlabs/openraft" },
  { name: "Walrus", icon: "/img/users/walrus.png", href: "https://github.com/nubskr/walrus" },
  { name: "RobustMQ", icon: "/img/users/robustmq.png", href: "https://github.com/robustmq/robustmq" },
  { name: "lnx", icon: "/img/users/lnx.png", href: "https://github.com/lnx-search/lnx" },
  { name: "Iceberg Rust", icon: "/img/users/iceberg-rust.png", href: "https://github.com/apache/iceberg-rust" },
  { name: "DataFusion Comet", icon: "/img/users/datafusion-comet.png", href: "https://github.com/apache/datafusion-comet" },
  { name: "Paimon Rust", icon: "/img/users/paimon-rust.png", href: "https://github.com/apache/paimon-rust" },
  { name: "zino", icon: "/img/users/zino.png", href: "https://github.com/zino-rs/zino" },
  { name: "kunobi-kache", icon: "/img/users/kunobi-kache.png", href: "https://github.com/kunobi-ninja/kache" },
  { name: "AgentENV", icon: "/img/users/agentenv.png", href: "https://github.com/kvcache-ai/AgentENV" },
];

// Where people add their own project (PR to the users list).
export const USERS_LIST_URL =
  "https://github.com/apache/opendal/blob/main/core/users.md";

// Curated, icon-backed slice of the 50+ supported services, grouped by family.
export const serviceGroups = [
  {
    category: "Object Storage",
    services: [
      { name: "s3", icon: "/img/services/s3.svg" },
      { name: "gcs", icon: "/img/services/gcs.png" },
      { name: "azblob", icon: "/img/services/azure.svg" },
      { name: "oss", icon: "/img/services/oss.svg" },
      { name: "cos", icon: "/img/services/cos.svg" },
      { name: "obs", icon: "/img/services/obs.png" },
      { name: "b2", icon: "/img/services/backblaze.png" },
      { name: "tos", icon: "/img/services/volcengine.png" },
    ],
  },
  {
    category: "File Storage",
    services: [
      { name: "fs", icon: "/img/services/opendal.svg" },
      { name: "hdfs", icon: "/img/services/hadoop.ico" },
      { name: "alluxio", icon: "/img/services/alluxio.svg" },
      { name: "goosefs", icon: "/img/services/goosefs.svg" },
      { name: "lakefs", icon: "/img/services/lakefs.ico" },
      { name: "ipfs", icon: "/img/services/ipfs.ico" },
      { name: "dbfs", icon: "/img/services/databricks.png" },
    ],
  },
  {
    category: "Cloud SaaS",
    services: [
      { name: "gdrive", icon: "/img/services/gdrive.png" },
      { name: "dropbox", icon: "/img/services/dropbox.ico" },
      { name: "onedrive", icon: "/img/services/onedrive.svg" },
      { name: "hf", icon: "/img/services/huggingface.ico" },
      { name: "github", icon: "/img/services/github.svg" },
      { name: "koofr", icon: "/img/services/koofr.ico" },
    ],
  },
  {
    category: "Protocols",
    services: [
      { name: "http", icon: "/img/services/http.png" },
      { name: "ftp", icon: "/img/services/ftp.png" },
      { name: "webdav", icon: "/img/services/webdav.png" },
      { name: "sftp", icon: "/img/services/sftp.png" },
    ],
  },
  {
    category: "Databases",
    services: [
      { name: "sqlite", icon: "/img/services/sqlite.ico" },
      { name: "mysql", icon: "/img/services/mysql.ico" },
      { name: "postgresql", icon: "/img/services/postgresql.ico" },
      { name: "mongodb", icon: "/img/services/mongodb.ico" },
      { name: "surrealdb", icon: "/img/services/surrealdb.svg" },
      { name: "d1", icon: "/img/services/cloudflare.ico" },
    ],
  },
  {
    category: "Key-Value",
    services: [
      { name: "redis", icon: "/img/services/redis.png" },
      { name: "etcd", icon: "/img/services/etcd.png" },
      { name: "rocksdb", icon: "/img/services/rocksdb.png" },
      { name: "memcached", icon: "/img/services/memcached.png" },
      { name: "tikv", icon: "/img/services/tikv.png" },
      { name: "foundationdb", icon: "/img/services/foundationdb.png" },
    ],
  },
];

// Each binding links to its docs landing page. Rust is the core crate and lives
// at /docs/core; every other binding lives under /docs/bindings/<dir>, where the
// dir matches the doc slug (e.g. Node.js -> nodejs, C++ -> cpp, .NET -> dotnet).
export const bindings = [
  { name: "Rust", icon: "/img/bindings/rust.svg", doc: "/docs/core" },
  { name: "Python", icon: "/img/bindings/python.svg", doc: "/docs/bindings/python" },
  { name: "Java", icon: "/img/bindings/java.svg", doc: "/docs/bindings/java" },
  { name: "Go", icon: "/img/bindings/go.svg", doc: "/docs/bindings/go" },
  { name: "Node.js", icon: "/img/bindings/nodejs.svg", doc: "/docs/bindings/nodejs" },
  { name: "C", icon: "/img/bindings/c.svg", doc: "/docs/bindings/c" },
  { name: "C++", icon: "/img/bindings/cpp.svg", doc: "/docs/bindings/cpp" },
  { name: ".NET", icon: "/img/bindings/dotnet.svg", doc: "/docs/bindings/dotnet" },
  { name: "Ruby", icon: "/img/bindings/ruby.svg", doc: "/docs/bindings/ruby" },
  { name: "PHP", icon: "/img/bindings/php.svg", doc: "/docs/bindings/php" },
  { name: "Swift", icon: "/img/bindings/swift.svg", doc: "/docs/bindings/swift" },
  { name: "Haskell", icon: "/img/bindings/haskell.svg", doc: "/docs/bindings/haskell" },
  { name: "OCaml", icon: "/img/bindings/ocaml.svg", doc: "/docs/bindings/ocaml" },
  { name: "Lua", icon: "/img/bindings/lua.svg", doc: "/docs/bindings/lua" },
  { name: "Dart", icon: "/img/bindings/dart.svg", doc: "/docs/bindings/dart" },
  { name: "D", icon: "/img/bindings/d.svg", doc: "/docs/bindings/d" },
  { name: "Zig", icon: "/img/bindings/zig.svg", doc: "/docs/bindings/zig" },
];
