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
op, _ := opendal.NewOperator(s3.Scheme, opendal.OperatorOptions{
    "bucket": "data",
})

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

// Layered example used in the Layers section.
export const layeredCode = `use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::Operator;

// Cross-cutting behavior composes onto any operator, in order.
let op = op
    .layer(RetryLayer::new())
    .layer(LoggingLayer::default());`;

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

// What OpenDAL can do, grouped for the capabilities section. Grounded in the
// core read/write/manage features (ranges, concurrency, multipart, listing …).
export const capabilityGroups = [
  {
    title: "Read",
    items: [
      "Whole-object or byte-range reads",
      "Concurrent, chunked fetching",
      "Auto-merge nearby ranges",
      "Resume interrupted reads",
      "Conditional & versioned reads",
    ],
  },
  {
    title: "Write",
    items: [
      "Streaming writes of any size",
      "Multipart upload",
      "Concurrent part uploads",
      "Append to existing objects",
      "Atomic create & overwrite guards",
    ],
  },
  {
    title: "Manage",
    items: [
      "Stat metadata without the body",
      "Lazy, recursive, paginated listing",
      "Batch & recursive delete",
      "Server-side copy & rename",
      "Presigned URLs for direct access",
    ],
  },
];

// Real adopters from the project's users lists (core + bindings), ordered by
// GitHub star count (highest first). Logos are the projects' GitHub org
// avatars, self-hosted under static/img/users/. Refresh with scripts as the
// ecosystem grows. The logo wall shows a responsive subset by viewport width.
export const usedBy = [
  { name: "Dify", icon: "/img/users/dify.png", href: "https://github.com/langgenius/dify" },
  { name: "Milvus", icon: "/img/users/milvus.png", href: "https://github.com/milvus-io/milvus" },
  { name: "Vector", icon: "/img/users/vector.png", href: "https://github.com/vectordotdev/vector" },
  { name: "QuestDB", icon: "/img/users/questdb.png", href: "https://github.com/questdb/questdb" },
  { name: "Databend", icon: "/img/users/databend.png", href: "https://github.com/databendlabs/databend" },
  { name: "RisingWave", icon: "/img/users/risingwave.png", href: "https://github.com/risingwavelabs/risingwave" },
  { name: "FileCodeBox", icon: "/img/users/filecodebox.png", href: "https://github.com/vastsa/FileCodeBox" },
  { name: "sccache", icon: "/img/users/sccache.png", href: "https://github.com/mozilla/sccache" },
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
    icon: "/img/layers/retry.svg",
    desc: "Recover from transient failures automatically.",
  },
  {
    name: "TimeoutLayer",
    icon: "/img/layers/timeout.svg",
    desc: "Bound slow or hanging operations.",
  },
  {
    name: "LoggingLayer",
    icon: "/img/layers/logging.svg",
    desc: "Emit structured operation logs.",
  },
  {
    name: "TracingLayer",
    icon: "/img/layers/tracing.svg",
    desc: "Trace requests across systems.",
  },
  {
    name: "MetricsLayer",
    icon: "/img/layers/metrics.svg",
    desc: "Export operation metrics.",
  },
  {
    name: "PrometheusLayer",
    icon: "/img/layers/prometheus.svg",
    desc: "Expose Prometheus metrics.",
  },
  {
    name: "Traffic Control",
    icon: "/img/layers/traffic-control.svg",
    desc: "Throttle and cap concurrency.",
  },
  {
    name: "FoyerLayer",
    icon: "/img/layers/cache.svg",
    desc: "Add hybrid cache behavior.",
  },
];

export const principles = [
  {
    title: "Open Community",
    body: "An ASF project governed by its PMC. Community over code, the Apache Way.",
  },
  {
    title: "Solid Foundation",
    body: "A dependable base you can trust for real-world storage operations.",
  },
  {
    title: "Fast Access",
    body: "Zero-overhead access — as fast as, or faster than, each native SDK.",
  },
  {
    title: "Object Storage First",
    body: "Designed and optimized for modern object storage from the ground up.",
  },
  {
    title: "Extensible Architecture",
    body: "Independent services, layers and bindings you can stack and extend.",
  },
];
