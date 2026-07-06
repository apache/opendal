"use strict";(self.webpackChunkopendal_website=self.webpackChunkopendal_website||[]).push([["4962"],{1516(e,a,i){i.d(a,{A:()=>s});let s={section:"section_kt1o",sectionSubtle:"sectionSubtle_K0DA",sectionHead:"sectionHead_eVd5",sectionTitle:"sectionTitle_lwdD",sectionLede:"sectionLede_zzJf",btn:"btn_KD3S",btnPrimary:"btnPrimary_ShrH",btnSecondary:"btnSecondary_eSsH",btnArrow:"btnArrow_rF6r",hero:"hero_CS2d",heroGrid:"heroGrid_hjMD",heroInner:"heroInner_LFP3",heroTitle:"heroTitle__rdc",heroTitleAccent:"heroTitleAccent_rq1g",heroLede:"heroLede_V0V0",heroActions:"heroActions_Y29m",heroStats:"heroStats_CzVl",heroStat:"heroStat_kHiT",heroStatValue:"heroStatValue_XFdq",heroStatLabel:"heroStatLabel_Tnfi",codeWindow:"codeWindow_H_TT",windowBar:"windowBar_cH2q",windowDots:"windowDots_lP2J",windowTitle:"windowTitle_qSEz",codeTabs:"codeTabs_IFtJ",codeTab:"codeTab_ezqM",codeTabActive:"codeTabActive_iWlr",codeBodyAnimated:"codeBodyAnimated_H1_S",codeBody:"codeBody_uchh",logoWall:"logoWall_JaQj",logoItem:"logoItem_zgCr",logoMark:"logoMark_cZlg",logoName:"logoName_bXrh",addLogo:"addLogo_RPG7",valueGrid:"valueGrid_Fb4n",valueCard:"valueCard_S3nd",valueIndex:"valueIndex_BYQX",valueCardTitle:"valueCardTitle_e8EB",valueCardBody:"valueCardBody_b0kH",capabilityExplorer:"capabilityExplorer_mh11",capabilityNav:"capabilityNav_teEQ",capabilityItem:"capabilityItem_hmsR",capabilityItemActive:"capabilityItemActive_FQ4p",capabilityText:"capabilityText__sai",capabilityItemTitle:"capabilityItemTitle_UuSB",capabilityItemBlurb:"capabilityItemBlurb_YA8T",capabilityArrow:"capabilityArrow_WPS_",capabilityPreview:"capabilityPreview_iofT",capabilityCodeBody:"capabilityCodeBody_aTbu",capabilityCodeFade:"capabilityCodeFade_jEBn",odlFade:"odlFade_QPuu",serviceGroups:"serviceGroups_KW9i",serviceGroup:"serviceGroup_S6Md",serviceGroupTitle:"serviceGroupTitle_JVVa",serviceChips:"serviceChips_VL8l",serviceChip:"serviceChip_RXO1",servicesFoot:"servicesFoot_ySXz",bindingGrid:"bindingGrid_EgYp",bindingCard:"bindingCard_kW8z",bindingName:"bindingName_bPSq",layerExplorer:"layerExplorer_I5Gf",layerGrid:"layerGrid_KLek",layerItem:"layerItem_Aan8",layerItemActive:"layerItemActive_Sgms",layerName:"layerName_M1g1",layerDesc:"layerDesc_ki_z",layerCodeBody:"layerCodeBody__gDc",finalCta:"finalCta_PaCJ",finalCtaInner:"finalCtaInner_nbDm",finalCtaTitle:"finalCtaTitle_N6o1",finalCtaLede:"finalCtaLede_DF_Q",finalCtaActions:"finalCtaActions_PgWz",finalEyebrow:"finalEyebrow_qzMi",finalCenter:"finalCenter_DT84",reveal:"reveal_SnRz",odlReveal:"odlReveal_smEt",heroAside:"heroAside_kUpl"}},3410(e,a,i){i.d(a,{A:()=>o});var s=i(2888),t=i(4358),r=i(1516),n=i(1684);function o({samples:e,title:a,equalize:i=!1}){let[c,l]=(0,s.useState)(0),d=(0,s.useId)(),m=e[c],p=m.install||a,h=(0,s.useRef)(null),[g,u]=(0,s.useState)(void 0);return(0,s.useEffect)(()=>{if(!i||!h.current)return;let e=()=>u(h.current?.offsetHeight);e();let a="u">typeof ResizeObserver?new ResizeObserver(e):null;return a&&h.current&&a.observe(h.current),()=>a?.disconnect()},[c,i]),(0,n.jsxs)("div",{className:r.A.codeWindow,children:[(0,n.jsxs)("div",{className:r.A.windowBar,children:[(0,n.jsxs)("div",{className:r.A.windowDots,"aria-hidden":"true",children:[(0,n.jsx)("span",{}),(0,n.jsx)("span",{}),(0,n.jsx)("span",{})]}),(0,n.jsx)("span",{className:r.A.windowTitle,children:p})]}),(0,n.jsx)("div",{className:r.A.codeTabs,role:"tablist","aria-label":"Choose a language",onKeyDown:function(a){if("ArrowRight"!==a.key&&"ArrowLeft"!==a.key)return;a.preventDefault();let i="ArrowRight"===a.key?1:-1;l(a=>(a+i+e.length)%e.length)},children:e.map((e,a)=>(0,n.jsx)("button",{type:"button",role:"tab",id:`${d}-tab-${e.id}`,"aria-selected":a===c,"aria-controls":`${d}-panel`,tabIndex:a===c?0:-1,className:`${r.A.codeTab} ${a===c?r.A.codeTabActive:""}`,onClick:()=>l(a),children:e.label},e.id))}),(0,n.jsx)("div",{className:`${r.A.codeBody} ${i?r.A.codeBodyAnimated:""}`,style:i&&null!=g?{height:g}:void 0,role:"tabpanel",id:`${d}-panel`,"aria-labelledby":`${d}-tab-${m.id}`,children:(0,n.jsx)("div",{ref:h,children:(0,n.jsx)(t.A,{language:m.language,children:m.code})})})]})}},6796(e,a,i){i.r(a),i.d(a,{default:()=>$});var s=i(2888),t=i(2338),r=i(2699),n=i(1e3),o=i(4358),c=i(3410),l=i(1516);let d="https://github.com/apache/opendal",m=[{value:"50+",label:"services"},{value:"17",label:"languages"},{value:"5k+",label:"stars"},{value:"10M+",label:"downloads"}],p=[{id:"rust",label:"Rust",language:"rust",install:"$ cargo add opendal",code:`use opendal::services::S3;
use opendal::Operator;

// Configure a backend once, then use one Operator.
let builder = S3::default().bucket("data");
let operator = Operator::new(builder)?.finish();

operator.write("hello.txt", "Hello, World!").await?;
let bytes = operator.read("hello.txt").await?;`},{id:"java",label:"Java",language:"java",install:"org.apache.opendal:opendal-java",code:`import org.apache.opendal.AsyncOperator;
import java.util.Map;

// Configure a backend once, then use one Operator.
var config = Map.of("bucket", "data");
try (var operator = AsyncOperator.of("s3", config)) {
    operator.write("hello.txt", "Hello, World!").join();
    byte[] data = operator.read("hello.txt").join();
}`},{id:"python",label:"Python",language:"python",install:"$ pip install opendal",code:`import opendal

# Configure a backend once, then use one Operator.
operator = opendal.Operator("s3", bucket="data")

operator.write("hello.txt", b"Hello, World!")
data = operator.read("hello.txt")`},{id:"node",label:"Node.js",language:"javascript",install:"$ npm install opendal",code:`import { Operator } from "opendal";

// Configure a backend once, then use one Operator.
const operator = new Operator("s3", { bucket: "data" });

await operator.write("hello.txt", "Hello, World!");
const data = await operator.read("hello.txt");`},{id:"ruby",label:"Ruby",language:"ruby",install:"$ gem install opendal",code:`require 'opendal'

# Configure a backend once, then use one Operator.
operator = OpenDAL::Operator.new("s3", { "bucket" => "data" })

operator.write("hello.txt", "Hello, World!")
data = operator.read("hello.txt")`},{id:"go",label:"Go",language:"go",install:"$ go get github.com/apache/opendal/bindings/go",code:`import (
    "github.com/apache/opendal-go-services/s3"
    opendal "github.com/apache/opendal/bindings/go"
)

// Configure a backend once, then use one Operator.
opts := opendal.OperatorOptions{"bucket": "data"}
op, _ := opendal.NewOperator(s3.Scheme, opts)

operator.Write("hello.txt", []byte("Hello, World!"))
data, _ := operator.Read("hello.txt")`},{id:"c",label:"C",language:"c",install:"opendal-c",code:`#include <opendal.h>

od_operator_options_t *options = od_operator_options_new();
od_operator_options_set(options, "bucket", "data");
od_operator_t *operator = od_operator_new("s3", options);

od_operator_write(operator, "hello.txt", "Hello, World!", 13);

char *data = NULL;
size_t size = 0;
od_operator_read(operator, "hello.txt", &data, &size);`},{id:"cpp",label:"C++",language:"cpp",install:"opendal-cpp",code:`#include <opendal.hpp>

opendal::Operator operator("s3", {{"bucket", "data"}});

std::string content = "Hello, World!";
operator.Write("hello.txt", content);

auto result = operator.Read("hello.txt");`}],h=[{index:"01",title:"One API, all storage",body:"Object storage, file systems, cloud SaaS, databases, protocols and key-value services reached through a single Operator and one mental model."},{index:"02",title:"Zero-cost core",body:"Built in Rust with composable services and layers. Compile in only the backends and capabilities you use, and pay for nothing else."},{index:"03",title:"Production-ready by composition",body:"Stack retry, timeout, logging, tracing, metrics, throttling and concurrency limits as reusable layers \u2014 no rewrites, no glue code."},{index:"04",title:"Open and extensible",body:"Add services, layers and language bindings without forking the model. Developed in the open and governed the Apache Way."}],g="https://docs.rs/opendal/latest/opendal",u=e=>`${g}/struct.Operator.html#method.${e}`,b=e=>`${g}/layers/struct.${e}.html`,y=[{title:"Read in parallel",blurb:"Fetch a byte range, or a whole object in concurrent chunks.",doc:u("read_with"),code:`let operator = Operator::new(S3::default().bucket("data"))?.finish();

// Read just the bytes you need.
let head = operator.read_with("logs/today").range(0..64 * 1024).await?;

// Or pull a large object in parallel chunks.
let object = operator
    .read_with("big.parquet")
    .concurrent(8)
    .chunk(8 * 1024 * 1024)
    .await?;`},{title:"Upload in parts",blurb:"Stream writes of any size as concurrent, multipart uploads.",doc:u("writer_with"),code:`let operator = Operator::new(S3::default().bucket("data"))?.finish();

// Open a multipart writer with 8 concurrent parts.
let mut writer = operator
    .writer_with("big.bin")
    .concurrent(8)
    .chunk(8 * 1024 * 1024)
    .await?;

// Stream any number of buffers; close flushes the rest.
writer.write(part_one).await?;
writer.write(part_two).await?;
writer.close().await?;`},{title:"Recover from failure",blurb:"Resume on retry, write atomically, and pin a version.",doc:`${g}/layers/struct.RetryLayer.html`,code:`// Retries automatically resume interrupted transfers.
let operator = Operator::new(S3::default().bucket("data"))?
    .layer(RetryLayer::new())
    .finish();

// Create only if absent.
operator.write_with("once.json", data).if_not_exists(true).await?;
// Read only if unchanged.
let doc = operator.read_with("doc").if_match(etag).await?;
// Pin an exact version.
let pinned = operator.read_with("doc").version(version_id).await?;`},{title:"Work with files",blurb:"Inspect, list, move, and share \u2014 without moving bytes.",doc:u("list_with"),code:`// Inspect a file without downloading it.
let meta = operator.stat("report.csv").await?;
// List a prefix, recursing lazily through the tree.
let mut entries = operator.lister_with("logs/").recursive(true).await?;
while let Some(entry) = entries.try_next().await? {
    println!("{}", entry.path());
}
// Copy on the server \u{2014} no download.
operator.copy("draft.md", "final.md").await?;
// Recursively delete a subtree.
operator.delete_with("tmp/").recursive(true).await?;
// Presign a temporary, shareable URL.
let ttl = Duration::from_secs(3600);
let url = operator.presign_read("report.csv", ttl).await?;`}],v=[{name:"Dify",icon:"/img/users/dify.png",href:"https://github.com/langgenius/dify"},{name:"RAGFlow",icon:"/img/users/ragflow.png",href:"https://github.com/infiniflow/ragflow"},{name:"Pathway",icon:"/img/users/pathway.png",href:"https://github.com/pathwaycom/pathway"},{name:"Vaultwarden",icon:"/img/users/vaultwarden.png",href:"https://github.com/dani-garcia/vaultwarden"},{name:"LlamaIndex",icon:"/img/users/llamaindex.png",href:"https://github.com/run-llama/llama_index"},{name:"Hasura",icon:"/img/users/hasura.png",href:"https://github.com/hasura/graphql-engine"},{name:"Vector",icon:"/img/users/vector.png",href:"https://github.com/vectordotdev/vector"},{name:"QuestDB",icon:"/img/users/questdb.png",href:"https://github.com/questdb/questdb"},{name:"WrenAI",icon:"/img/users/wrenai.png",href:"https://github.com/Canner/WrenAI"},{name:"Quickwit",icon:"/img/users/quickwit.png",href:"https://github.com/quickwit-oss/quickwit"},{name:"SeaTunnel",icon:"/img/users/seatunnel.png",href:"https://github.com/apache/seatunnel"},{name:"Databend",icon:"/img/users/databend.png",href:"https://github.com/databendlabs/databend"},{name:"RisingWave",icon:"/img/users/risingwave.png",href:"https://github.com/risingwavelabs/risingwave"},{name:"Loco",icon:"/img/users/loco.png",href:"https://github.com/loco-rs/loco"},{name:"sccache",icon:"/img/users/sccache.png",href:"https://github.com/mozilla/sccache"},{name:"Lance",icon:"/img/users/lance.png",href:"https://github.com/lance-format/lance"},{name:"GreptimeDB",icon:"/img/users/greptimedb.png",href:"https://github.com/GreptimeTeam/greptimedb"},{name:"Daft",icon:"/img/users/daft.png",href:"https://github.com/Eventual-Inc/Daft"},{name:"CrateDB",icon:"/img/users/cratedb.png",href:"https://github.com/crate/crate"},{name:"Pants",icon:"/img/users/pants.png",href:"https://github.com/pantsbuild/pants"},{name:"rustic",icon:"/img/users/rustic.png",href:"https://github.com/rustic-rs/rustic"},{name:"SlateDB",icon:"/img/users/slatedb.png",href:"https://github.com/slatedb/slatedb"},{name:"Gravitino",icon:"/img/users/gravitino.png",href:"https://github.com/apache/gravitino"},{name:"Spice.ai",icon:"/img/users/spiceai.png",href:"https://github.com/spiceai/spiceai"},{name:"Kubeflow Trainer",icon:"/img/users/kubeflow-trainer.png",href:"https://github.com/kubeflow/trainer"},{name:"OctoBase",icon:"/img/users/octobase.png",href:"https://github.com/toeverything/OctoBase"},{name:"Openraft",icon:"/img/users/openraft.png",href:"https://github.com/databendlabs/openraft"},{name:"Walrus",icon:"/img/users/walrus.png",href:"https://github.com/nubskr/walrus"},{name:"RobustMQ",icon:"/img/users/robustmq.png",href:"https://github.com/robustmq/robustmq"},{name:"lnx",icon:"/img/users/lnx.png",href:"https://github.com/lnx-search/lnx"},{name:"Iceberg Rust",icon:"/img/users/iceberg-rust.png",href:"https://github.com/apache/iceberg-rust"},{name:"DataFusion Comet",icon:"/img/users/datafusion-comet.png",href:"https://github.com/apache/datafusion-comet"},{name:"Paimon Rust",icon:"/img/users/paimon-rust.png",href:"https://github.com/apache/paimon-rust"},{name:"zino",icon:"/img/users/zino.png",href:"https://github.com/zino-rs/zino"}],x=[{category:"Object Storage",services:[{name:"s3",icon:"/img/services/s3.svg"},{name:"gcs",icon:"/img/services/gcs.png"},{name:"azblob",icon:"/img/services/azure.svg"},{name:"oss",icon:"/img/services/oss.svg"},{name:"cos",icon:"/img/services/cos.svg"},{name:"obs",icon:"/img/services/obs.png"},{name:"b2",icon:"/img/services/backblaze.png"},{name:"tos",icon:"/img/services/volcengine.png"}]},{category:"File Storage",services:[{name:"fs",icon:"/img/services/opendal.svg"},{name:"hdfs",icon:"/img/services/hadoop.ico"},{name:"alluxio",icon:"/img/services/alluxio.svg"},{name:"goosefs",icon:"/img/services/goosefs.svg"},{name:"lakefs",icon:"/img/services/lakefs.ico"},{name:"ipfs",icon:"/img/services/ipfs.ico"},{name:"dbfs",icon:"/img/services/databricks.png"}]},{category:"Cloud SaaS",services:[{name:"gdrive",icon:"/img/services/gdrive.png"},{name:"dropbox",icon:"/img/services/dropbox.ico"},{name:"onedrive",icon:"/img/services/onedrive.svg"},{name:"hf",icon:"/img/services/huggingface.ico"},{name:"github",icon:"/img/services/github.svg"},{name:"koofr",icon:"/img/services/koofr.ico"}]},{category:"Protocols",services:[{name:"http",icon:"/img/services/http.png"},{name:"ftp",icon:"/img/services/ftp.png"},{name:"webdav",icon:"/img/services/webdav.png"},{name:"sftp",icon:"/img/services/sftp.png"}]},{category:"Databases",services:[{name:"sqlite",icon:"/img/services/sqlite.ico"},{name:"mysql",icon:"/img/services/mysql.ico"},{name:"postgresql",icon:"/img/services/postgresql.ico"},{name:"mongodb",icon:"/img/services/mongodb.ico"},{name:"surrealdb",icon:"/img/services/surrealdb.svg"},{name:"d1",icon:"/img/services/cloudflare.ico"}]},{category:"Key-Value",services:[{name:"redis",icon:"/img/services/redis.png"},{name:"etcd",icon:"/img/services/etcd.png"},{name:"rocksdb",icon:"/img/services/rocksdb.png"},{name:"memcached",icon:"/img/services/memcached.png"},{name:"tikv",icon:"/img/services/tikv.png"},{name:"foundationdb",icon:"/img/services/foundationdb.png"}]}],f=[{name:"Rust",icon:"/img/bindings/rust.svg",doc:"/docs/core"},{name:"Python",icon:"/img/bindings/python.svg",doc:"/docs/bindings/python"},{name:"Java",icon:"/img/bindings/java.svg",doc:"/docs/bindings/java"},{name:"Go",icon:"/img/bindings/go.svg",doc:"/docs/bindings/go"},{name:"Node.js",icon:"/img/bindings/nodejs.svg",doc:"/docs/bindings/nodejs"},{name:"C",icon:"/img/bindings/c.svg",doc:"/docs/bindings/c"},{name:"C++",icon:"/img/bindings/cpp.svg",doc:"/docs/bindings/cpp"},{name:".NET",icon:"/img/bindings/dotnet.svg",doc:"/docs/bindings/dotnet"},{name:"Ruby",icon:"/img/bindings/ruby.svg",doc:"/docs/bindings/ruby"},{name:"PHP",icon:"/img/bindings/php.svg",doc:"/docs/bindings/php"},{name:"Swift",icon:"/img/bindings/swift.svg",doc:"/docs/bindings/swift"},{name:"Haskell",icon:"/img/bindings/haskell.svg",doc:"/docs/bindings/haskell"},{name:"OCaml",icon:"/img/bindings/ocaml.svg",doc:"/docs/bindings/ocaml"},{name:"Lua",icon:"/img/bindings/lua.svg",doc:"/docs/bindings/lua"},{name:"Dart",icon:"/img/bindings/dart.svg",doc:"/docs/bindings/dart"},{name:"D",icon:"/img/bindings/d.svg",doc:"/docs/bindings/d"},{name:"Zig",icon:"/img/bindings/zig.svg",doc:"/docs/bindings/zig"}],A=[{name:"RetryLayer",desc:"Recover from transient failures automatically.",doc:b("RetryLayer"),code:`use opendal::layers::RetryLayer;

// Exponential backoff with jitter; interrupted
// reads and writes resume where they left off.
let operator = operator.layer(
    RetryLayer::new().with_max_times(5).with_jitter(),
);`},{name:"TimeoutLayer",desc:"Bound slow or hanging operations.",doc:b("TimeoutLayer"),code:`use opendal::layers::TimeoutLayer;
use std::time::Duration;

// Abort operations that stall past a deadline.
let operator = operator.layer(
    TimeoutLayer::new()
        .with_timeout(Duration::from_secs(10)),
);`},{name:"LoggingLayer",desc:"Emit structured operation logs.",doc:b("LoggingLayer"),code:`use opendal::layers::LoggingLayer;

// Structured logs for every operation, via the
// standard log crate facade.
let operator = operator.layer(LoggingLayer::default());`},{name:"TracingLayer",desc:"Trace requests across systems.",doc:b("TracingLayer"),code:`use opendal::layers::TracingLayer;

// One span per operation, into whatever
// tracing subscriber your app installs.
let operator = operator.layer(TracingLayer::new());`},{name:"MetricsLayer",desc:"Export operation metrics.",doc:b("MetricsLayer"),code:`use opendal::layers::MetricsLayer;

// Latency and throughput via the metrics crate
// facade \u{2014} plug in any exporter you like.
let operator = operator.layer(MetricsLayer::default());`},{name:"PrometheusLayer",desc:"Expose Prometheus metrics.",doc:b("PrometheusLayer"),code:`use opendal::layers::PrometheusLayer;

// Register operation metrics into a Prometheus
// registry you already expose.
let registry = prometheus::default_registry();
let operator = operator.layer(
    PrometheusLayer::builder().register(registry)?,
);`},{name:"ConcurrentLimitLayer",desc:"Cap in-flight concurrency.",doc:b("ConcurrentLimitLayer"),code:`use opendal::layers::ConcurrentLimitLayer;

// Cap how many operations hit the backend at once \u{2014}
// back-pressure for the whole Operator.
let operator = operator.layer(ConcurrentLimitLayer::new(1024));`},{name:"ThrottleLayer",desc:"Throttle I/O bandwidth.",doc:b("ThrottleLayer"),code:`use opendal::layers::ThrottleLayer;

// Token-bucket bandwidth limit: ~10 MiB/s steady,
// with headroom to burst for short spikes.
let operator = operator.layer(ThrottleLayer::new(
    10 * 1024 * 1024,
    32 * 1024 * 1024,
));`}];var w=i(1684);function j(){return(0,w.jsxs)("header",{className:l.A.hero,children:[(0,w.jsx)("div",{className:`${l.A.heroGrid} odl-grid-bg`,"aria-hidden":"true"}),(0,w.jsx)("div",{className:"odl-container",children:(0,w.jsxs)("div",{className:l.A.heroInner,children:[(0,w.jsxs)("div",{children:[(0,w.jsx)("span",{className:"odl-eyebrow",children:"Apache OpenDAL\u2122 \u2014 Open Data Access Layer"}),(0,w.jsxs)("h1",{className:l.A.heroTitle,children:["One Layer,",(0,w.jsx)("br",{}),(0,w.jsx)("span",{className:l.A.heroTitleAccent,children:"All Storage."})]}),(0,w.jsx)("p",{className:l.A.heroLede,children:"One zero-cost Operator for object storage, file systems, databases and more \u2014 in the language you already ship."}),(0,w.jsxs)("div",{className:l.A.heroActions,children:[(0,w.jsxs)(r.A,{className:`${l.A.btn} ${l.A.btnPrimary}`,to:"/docs/",children:["Get started ",(0,w.jsx)("span",{className:l.A.btnArrow,children:"\u2192"})]}),(0,w.jsx)(r.A,{className:`${l.A.btn} ${l.A.btnSecondary}`,to:d,children:"View on GitHub"})]}),(0,w.jsx)("div",{className:l.A.heroStats,children:m.map(e=>(0,w.jsxs)("div",{className:l.A.heroStat,children:[(0,w.jsx)("span",{className:l.A.heroStatValue,children:e.value}),(0,w.jsx)("span",{className:l.A.heroStatLabel,children:e.label})]},e.label))})]}),(0,w.jsx)("div",{className:l.A.heroAside,children:(0,w.jsx)(c.A,{samples:p,title:"quickstart",equalize:!0})})]})})]})}function N(){let{withBaseUrl:e}=(0,n.hH)();return(0,w.jsx)("section",{className:`${l.A.section} ${l.A.sectionSubtle}`,children:(0,w.jsxs)("div",{className:"odl-container",children:[(0,w.jsxs)("div",{className:l.A.sectionHead,children:[(0,w.jsx)("span",{className:"odl-eyebrow",children:"Used by"}),(0,w.jsx)("h2",{className:l.A.sectionTitle,children:"Powering AI, analytics, and real-time data"}),(0,w.jsx)("p",{className:l.A.sectionLede,children:"OpenDAL runs in production across the open-source ecosystem. These are some of the projects that build on it."})]}),(0,w.jsxs)("ul",{className:l.A.logoWall,children:[v.map(a=>(0,w.jsx)("li",{children:(0,w.jsxs)(r.A,{className:l.A.logoItem,to:a.href,title:a.name,children:[(0,w.jsx)("img",{className:l.A.logoMark,src:e(a.icon),alt:"",width:"28",height:"28",loading:"lazy"}),(0,w.jsx)("span",{className:l.A.logoName,children:a.name})]})},a.name)),(0,w.jsx)("li",{children:(0,w.jsx)(r.A,{className:l.A.addLogo,to:"https://github.com/apache/opendal/blob/main/core/users.md",children:"+ add your logo"})})]})]})})}function _(){return(0,w.jsx)("section",{className:l.A.section,children:(0,w.jsxs)("div",{className:"odl-container",children:[(0,w.jsxs)("div",{className:l.A.sectionHead,children:[(0,w.jsx)("span",{className:"odl-eyebrow",children:"Why OpenDAL"}),(0,w.jsx)("h2",{className:l.A.sectionTitle,children:"A storage layer you can build production on."}),(0,w.jsxs)("p",{className:l.A.sectionLede,children:["OpenDAL turns one vision \u2014 ",(0,w.jsx)("em",{children:"One Layer, All Storage"})," \u2014 into a practical foundation for applications, libraries and data systems."]})]}),(0,w.jsx)("div",{className:`${l.A.valueGrid} ${l.A.reveal}`,children:h.map(e=>(0,w.jsxs)("article",{className:l.A.valueCard,children:[(0,w.jsx)("span",{className:l.A.valueIndex,children:e.index}),(0,w.jsx)("h3",{className:l.A.valueCardTitle,children:e.title}),(0,w.jsx)("p",{className:l.A.valueCardBody,children:e.body})]},e.index))})]})})}function L(){let[e,a]=(0,s.useState)(y["0"]);return(0,w.jsx)("section",{className:`${l.A.section} ${l.A.sectionSubtle}`,children:(0,w.jsxs)("div",{className:"odl-container",children:[(0,w.jsxs)("div",{className:l.A.sectionHead,children:[(0,w.jsx)("span",{className:"odl-eyebrow",children:"Capabilities"}),(0,w.jsx)("h2",{className:l.A.sectionTitle,children:"Configure once. Access anything."}),(0,w.jsx)("p",{className:l.A.sectionLede,children:"One Operator is a full toolkit for real-world data \u2014 read and write at scale, recover from failures, and work with files \u2014 the same way on every backend."})]}),(0,w.jsxs)("div",{className:l.A.capabilityExplorer,children:[(0,w.jsx)("ul",{className:l.A.capabilityNav,children:y.map(i=>{let s=e.title===i.title;return(0,w.jsx)("li",{children:(0,w.jsxs)(r.A,{className:`${l.A.capabilityItem} ${s?l.A.capabilityItemActive:""}`,to:i.doc,target:"_blank",rel:"noreferrer","aria-current":s?"true":void 0,onMouseEnter:()=>a(i),onFocus:()=>a(i),children:[(0,w.jsxs)("span",{className:l.A.capabilityText,children:[(0,w.jsx)("span",{className:l.A.capabilityItemTitle,children:i.title}),(0,w.jsx)("span",{className:l.A.capabilityItemBlurb,children:i.blurb})]}),(0,w.jsx)("span",{className:l.A.capabilityArrow,"aria-hidden":"true",children:"\u2197"})]})},i.title)})}),(0,w.jsx)("div",{className:l.A.capabilityPreview,children:(0,w.jsxs)("div",{className:l.A.codeWindow,children:[(0,w.jsxs)("div",{className:l.A.windowBar,children:[(0,w.jsxs)("div",{className:l.A.windowDots,"aria-hidden":"true",children:[(0,w.jsx)("span",{}),(0,w.jsx)("span",{}),(0,w.jsx)("span",{})]}),(0,w.jsx)("span",{className:l.A.windowTitle,children:e.title})]}),(0,w.jsx)("div",{className:`${l.A.codeBody} ${l.A.capabilityCodeBody}`,children:(0,w.jsx)("div",{className:l.A.capabilityCodeFade,children:(0,w.jsx)(o.A,{language:"rust",children:e.code})},e.title)})]})})]})]})})}function k(){let{withBaseUrl:e}=(0,n.hH)();return(0,w.jsx)("section",{className:l.A.section,children:(0,w.jsxs)("div",{className:"odl-container",children:[(0,w.jsxs)("div",{className:l.A.sectionHead,children:[(0,w.jsx)("span",{className:"odl-eyebrow",children:"Services"}),(0,w.jsx)("h2",{className:l.A.sectionTitle,children:"50+ storage services, one interface."}),(0,w.jsx)("p",{className:l.A.sectionLede,children:"Enable only the backends your application needs. The Operator contract stays identical across every one."})]}),(0,w.jsx)("div",{className:`${l.A.serviceGroups} ${l.A.reveal}`,children:x.map(a=>(0,w.jsxs)("div",{className:l.A.serviceGroup,children:[(0,w.jsx)("h3",{className:l.A.serviceGroupTitle,children:a.category}),(0,w.jsx)("div",{className:l.A.serviceChips,children:a.services.map(a=>(0,w.jsxs)(r.A,{className:l.A.serviceChip,to:`/services/${a.name}`,children:[(0,w.jsx)("img",{src:e(a.icon),alt:"",width:"15",height:"15",loading:"lazy"}),a.name]},a.name))})]},a.category))}),(0,w.jsx)("div",{className:l.A.servicesFoot,children:(0,w.jsxs)(r.A,{className:`${l.A.btn} ${l.A.btnSecondary}`,to:"/docs/",children:["Browse all services ",(0,w.jsx)("span",{className:l.A.btnArrow,children:"\u2192"})]})})]})})}function C(){let{withBaseUrl:e}=(0,n.hH)();return(0,w.jsx)("section",{className:`${l.A.section} ${l.A.sectionSubtle}`,children:(0,w.jsxs)("div",{className:"odl-container",children:[(0,w.jsxs)("div",{className:l.A.sectionHead,children:[(0,w.jsx)("span",{className:"odl-eyebrow",children:"Bindings"}),(0,w.jsx)("h2",{className:l.A.sectionTitle,children:"Ship it in your language."}),(0,w.jsx)("p",{className:l.A.sectionLede,children:"Each binding exposes the same OpenDAL service model while following its own ecosystem's conventions."})]}),(0,w.jsx)("div",{className:`${l.A.bindingGrid} ${l.A.reveal}`,children:f.map(a=>(0,w.jsxs)(r.A,{className:l.A.bindingCard,to:a.doc,children:[(0,w.jsx)("img",{src:e(a.icon),alt:"",width:"30",height:"30",loading:"lazy"}),(0,w.jsx)("span",{className:l.A.bindingName,children:a.name})]},a.name))})]})})}function S(){let[e,a]=(0,s.useState)(A["0"]);return(0,w.jsx)("section",{className:l.A.section,children:(0,w.jsxs)("div",{className:"odl-container",children:[(0,w.jsxs)("div",{className:l.A.sectionHead,children:[(0,w.jsx)("span",{className:"odl-eyebrow",children:"Layers"}),(0,w.jsx)("h2",{className:l.A.sectionTitle,children:"Production behavior, composed \u2014 not coded."}),(0,w.jsx)("p",{className:l.A.sectionLede,children:"Stack cross-cutting concerns as reusable layers. The order is explicit and the core stays zero-cost."})]}),(0,w.jsxs)("div",{className:l.A.layerExplorer,children:[(0,w.jsx)("div",{className:l.A.layerGrid,children:A.map(i=>{let s=e.name===i.name;return(0,w.jsxs)(r.A,{className:`${l.A.layerItem} ${s?l.A.layerItemActive:""}`,to:i.doc,target:"_blank",rel:"noreferrer","aria-current":s?"true":void 0,onMouseEnter:()=>a(i),onFocus:()=>a(i),children:[(0,w.jsx)("span",{className:l.A.layerName,children:i.name}),(0,w.jsx)("span",{className:l.A.layerDesc,children:i.desc})]},i.name)})}),(0,w.jsx)("div",{className:l.A.capabilityPreview,children:(0,w.jsxs)("div",{className:l.A.codeWindow,children:[(0,w.jsxs)("div",{className:l.A.windowBar,children:[(0,w.jsxs)("div",{className:l.A.windowDots,"aria-hidden":"true",children:[(0,w.jsx)("span",{}),(0,w.jsx)("span",{}),(0,w.jsx)("span",{})]}),(0,w.jsx)("span",{className:l.A.windowTitle,children:e.name})]}),(0,w.jsx)("div",{className:`${l.A.codeBody} ${l.A.layerCodeBody}`,children:(0,w.jsx)("div",{className:l.A.capabilityCodeFade,children:(0,w.jsx)(o.A,{language:"rust",children:e.code})},e.name)})]})})]})]})})}function T(){return(0,w.jsx)("section",{className:`${l.A.section} ${l.A.sectionSubtle}`,children:(0,w.jsx)("div",{className:"odl-container",children:(0,w.jsx)("div",{className:l.A.finalCta,children:(0,w.jsxs)("div",{className:`${l.A.finalCtaInner} ${l.A.finalCenter}`,children:[(0,w.jsx)("span",{className:`odl-eyebrow ${l.A.finalEyebrow}`,children:"Start building"}),(0,w.jsx)("h2",{className:l.A.finalCtaTitle,children:"One layer for all your storage."}),(0,w.jsx)("p",{className:l.A.finalCtaLede,children:"Join the infrastructure builders, platform teams and application developers accessing data freely, painlessly and efficiently."}),(0,w.jsxs)("div",{className:l.A.finalCtaActions,children:[(0,w.jsxs)(r.A,{className:`${l.A.btn} ${l.A.btnPrimary}`,to:"/docs/",children:["Get started ",(0,w.jsx)("span",{className:l.A.btnArrow,children:"\u2192"})]}),(0,w.jsx)(r.A,{className:`${l.A.btn} ${l.A.btnSecondary}`,to:d,children:"Star on GitHub"}),(0,w.jsx)(r.A,{className:`${l.A.btn} ${l.A.btnSecondary}`,to:"https://discord.gg/XQy8yGR2dg",children:"Join Discord"})]})]})})})})}function $(){return(0,w.jsx)(t.A,{title:"One Layer, All Storage",description:"Apache OpenDAL\u2122 is an Open Data Access Layer that gives every language a unified, zero-cost way to access object storage, file systems, cloud SaaS, databases, protocols and key-value services.",children:(0,w.jsxs)("main",{children:[(0,w.jsx)(j,{}),(0,w.jsx)(N,{}),(0,w.jsx)(_,{}),(0,w.jsx)(L,{}),(0,w.jsx)(k,{}),(0,w.jsx)(C,{}),(0,w.jsx)(S,{}),(0,w.jsx)(T,{})]})})}}}]);