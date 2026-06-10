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

// Same Operator contract, one mental model, every ecosystem.
export const codeSamples = [
  {
    id: "rust",
    label: "Rust",
    language: "rust",
    install: "$ cargo add opendal",
    code: `use opendal::services::S3;
use opendal::Operator;

// Configure a backend once, then use one Operator.
let builder = S3::default().bucket("data");
let op = Operator::new(builder)?.finish();

op.write("hello.txt", "Hello, World!").await?;
let bytes = op.read("hello.txt").await?;`,
  },
  {
    id: "python",
    label: "Python",
    language: "python",
    install: "$ pip install opendal",
    code: `import opendal

# Configure a backend once, then use one Operator.
op = opendal.Operator("s3", bucket="data")

op.write("hello.txt", b"Hello, World!")
data = op.read("hello.txt")`,
  },
  {
    id: "go",
    label: "Go",
    language: "go",
    install: "$ go get github.com/apache/opendal/bindings/go",
    code: `import (
    "github.com/apache/opendal-go-services/s3"
    opendal "github.com/apache/opendal/bindings/go"
)

// Configure a backend once, then use one Operator.
opts := opendal.OperatorOptions{"bucket": "data"}
op, _ := opendal.NewOperator(s3.Scheme, opts)

op.Write("hello.txt", []byte("Hello, World!"))
data, _ := op.Read("hello.txt")`,
  },
  {
    id: "java",
    label: "Java",
    language: "java",
    install: "org.apache.opendal:opendal-java",
    code: `import org.apache.opendal.AsyncOperator;
import java.util.Map;

// Configure a backend once, then use one Operator.
var conf = Map.of("bucket", "data");
try (var op = AsyncOperator.of("s3", conf)) {
    op.write("hello.txt", "Hello, World!").join();
    byte[] data = op.read("hello.txt").join();
}`,
  },
  {
    id: "node",
    label: "Node.js",
    language: "javascript",
    install: "$ npm install opendal",
    code: `import { Operator } from "opendal";

// Configure a backend once, then use one Operator.
const op = new Operator("s3", { bucket: "data" });

await op.write("hello.txt", "Hello, World!");
const data = await op.read("hello.txt");`,
  },
];

export const valueProps = [
  {
    index: "01",
    title: "One API, all storage",
    body: "Object storage, file systems, cloud SaaS, databases, protocols and key-value services reached through a single Operator and one mental model.",
  },
  {
    index: "02",
    title: "Zero-cost core",
    body: "Built in Rust with composable services and layers. Compile in only the backends and capabilities you use, and pay for nothing else.",
  },
  {
    index: "03",
    title: "Production-ready by composition",
    body: "Stack retry, timeout, logging, tracing, metrics, throttling and concurrency limits as reusable layers — no rewrites, no glue code.",
  },
  {
    index: "04",
    title: "Open and extensible",
    body: "Add services, layers and language bindings without forking the model. Developed in the open and governed the Apache Way.",
  },
];

// What OpenDAL can do — grouped for the capabilities explorer. Each item pairs
// a minimal, real snippet with its docs.rs reference. Grounded in the core
// read / write / manage APIs (verified against types/operator + types/options).
const RS = "https://docs.rs/opendal/latest/opendal";
const opDoc = (method) => `${RS}/struct.Operator.html#method.${method}`;
const layerDoc = (name) => `${RS}/layers/struct.${name}.html`;

export const capabilityThemes = [
  {
    title: "Read in parallel",
    blurb: "Fetch a byte range, or a whole object in concurrent chunks.",
    doc: opDoc("read_with"),
    code: `let op = Operator::new(S3::default().bucket("data"))?.finish();

// Read just the bytes you need.
let head = op.read_with("logs/today").range(0..64 * 1024).await?;

// Or pull a large object in parallel chunks.
let full = op
    .read_with("big.parquet")
    .concurrent(8)
    .chunk(8 * 1024 * 1024)
    .await?;`,
  },
  {
    title: "Upload in parts",
    blurb: "Stream writes of any size as concurrent, multipart uploads.",
    doc: opDoc("writer_with"),
    code: `let op = Operator::new(S3::default().bucket("data"))?.finish();

// Open a multipart writer with 8 concurrent parts.
let mut w = op
    .writer_with("big.bin")
    .concurrent(8)
    .chunk(8 * 1024 * 1024)
    .await?;

// Stream any number of buffers; close flushes the rest.
w.write(part_one).await?;
w.write(part_two).await?;
w.close().await?;`,
  },
  {
    title: "Recover from failure",
    blurb: "Resume on retry, write atomically, and pin a version.",
    doc: `${RS}/layers/struct.RetryLayer.html`,
    code: `// Retries automatically resume interrupted transfers.
let op = Operator::new(S3::default().bucket("data"))?
    .layer(RetryLayer::new())
    .finish();

// Create only if absent.
op.write_with("once.json", data).if_not_exists(true).await?;
// Read only if unchanged.
let doc = op.read_with("doc").if_match(etag).await?;
// Pin an exact version.
let pinned = op.read_with("doc").version(version_id).await?;`,
  },
  {
    title: "Work with files",
    blurb: "Inspect, list, move, and share — without moving bytes.",
    doc: opDoc("list_with"),
    code: `// Inspect a file without downloading it.
let meta = op.stat("report.csv").await?;
// List a prefix, recursing lazily through the tree.
let mut entries = op.lister_with("logs/").recursive(true).await?;
while let Some(entry) = entries.try_next().await? {
    println!("{}", entry.path());
}
// Copy on the server — no download.
op.copy("draft.md", "final.md").await?;
// Recursively delete a subtree.
op.delete_with("tmp/").recursive(true).await?;
// Presign a temporary, shareable URL.
let ttl = Duration::from_secs(3600);
let url = op.presign_read("report.csv", ttl).await?;`,
  },
];

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

export const bindings = [
  { name: "Rust", icon: "/img/bindings/rust.svg" },
  { name: "Python", icon: "/img/bindings/python.svg" },
  { name: "Java", icon: "/img/bindings/java.svg" },
  { name: "Go", icon: "/img/bindings/go.svg" },
  { name: "Node.js", icon: "/img/bindings/nodejs.svg" },
  { name: "C", icon: "/img/bindings/c.svg" },
  { name: "C++", icon: "/img/bindings/cpp.svg" },
  { name: ".NET", icon: "/img/bindings/dotnet.svg" },
  { name: "Ruby", icon: "/img/bindings/ruby.svg" },
  { name: "PHP", icon: "/img/bindings/php.svg" },
  { name: "Swift", icon: "/img/bindings/swift.svg" },
  { name: "Haskell", icon: "/img/bindings/haskell.svg" },
  { name: "OCaml", icon: "/img/bindings/ocaml.svg" },
  { name: "Lua", icon: "/img/bindings/lua.svg" },
  { name: "Dart", icon: "/img/bindings/dart.svg" },
  { name: "D", icon: "/img/bindings/d.svg" },
  { name: "Zig", icon: "/img/bindings/zig.svg" },
];

export const layers = [
  {
    name: "RetryLayer",
    desc: "Recover from transient failures automatically.",
    doc: layerDoc("RetryLayer"),
    code: `use opendal::layers::RetryLayer;

// Exponential backoff with jitter; interrupted
// reads and writes resume where they left off.
let op = op.layer(
    RetryLayer::new().with_max_times(5).with_jitter(),
);`,
  },
  {
    name: "TimeoutLayer",
    desc: "Bound slow or hanging operations.",
    doc: layerDoc("TimeoutLayer"),
    code: `use opendal::layers::TimeoutLayer;
use std::time::Duration;

// Abort operations that stall past a deadline.
let op = op.layer(
    TimeoutLayer::new()
        .with_timeout(Duration::from_secs(10)),
);`,
  },
  {
    name: "LoggingLayer",
    desc: "Emit structured operation logs.",
    doc: layerDoc("LoggingLayer"),
    code: `use opendal::layers::LoggingLayer;

// Structured logs for every operation, via the
// standard log crate facade.
let op = op.layer(LoggingLayer::default());`,
  },
  {
    name: "TracingLayer",
    desc: "Trace requests across systems.",
    doc: layerDoc("TracingLayer"),
    code: `use opendal::layers::TracingLayer;

// One span per operation, into whatever
// tracing subscriber your app installs.
let op = op.layer(TracingLayer::new());`,
  },
  {
    name: "MetricsLayer",
    desc: "Export operation metrics.",
    doc: layerDoc("MetricsLayer"),
    code: `use opendal::layers::MetricsLayer;

// Latency and throughput via the metrics crate
// facade — plug in any exporter you like.
let op = op.layer(MetricsLayer::default());`,
  },
  {
    name: "PrometheusLayer",
    desc: "Expose Prometheus metrics.",
    doc: layerDoc("PrometheusLayer"),
    code: `use opendal::layers::PrometheusLayer;

// Register operation metrics into a Prometheus
// registry you already expose.
let registry = prometheus::default_registry();
let op = op.layer(
    PrometheusLayer::builder().register(registry)?,
);`,
  },
  {
    name: "ConcurrentLimitLayer",
    desc: "Cap in-flight concurrency.",
    doc: layerDoc("ConcurrentLimitLayer"),
    code: `use opendal::layers::ConcurrentLimitLayer;

// Cap how many operations hit the backend at once —
// back-pressure for the whole Operator.
let op = op.layer(ConcurrentLimitLayer::new(1024));`,
  },
  {
    name: "ThrottleLayer",
    desc: "Throttle I/O bandwidth.",
    doc: layerDoc("ThrottleLayer"),
    code: `use opendal::layers::ThrottleLayer;

// Token-bucket bandwidth limit: ~10 MiB/s steady,
// with headroom to burst for short spikes.
let op = op.layer(ThrottleLayer::new(
    10 * 1024 * 1024,
    32 * 1024 * 1024,
));`,
  },
];
