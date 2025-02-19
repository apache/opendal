# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

<!-- Release notes generated with: gh release create v_draft --generate-notes --draft -->

## [v0.52.0] - 2025-02-19

### Added
* feat(services/s3): Added crc64nvme for s3 by @geetanshjuneja in https://github.com/apache/opendal/pull/5580
* feat(services-fs): Support write-if-not-exists in fs backend by @SergeiPatiakin in https://github.com/apache/opendal/pull/5605
* feat(services/gcs): Impl content-encoding support for GCS stat, write and presign by @wlinna in https://github.com/apache/opendal/pull/5610
* feat(bindings/ruby): add lister by @erickguan in https://github.com/apache/opendal/pull/5600
* feat(services/swift): Added user metadata support for swift service by @zhaohaidao in https://github.com/apache/opendal/pull/5601
* feat: Implement github actions cache service v2 support by @Xuanwo in https://github.com/apache/opendal/pull/5633
* feat(core)!: implement write returns metadata by @meteorgan in https://github.com/apache/opendal/pull/5562
* feat(bindings/python): let path can be PathLike by @asukaminato0721 in https://github.com/apache/opendal/pull/5636
* feat(bindings/python): add exists by @asukaminato0721 in https://github.com/apache/opendal/pull/5637
### Changed
* refactor: Remove dead services libsql by @Xuanwo in https://github.com/apache/opendal/pull/5616
### Fixed
* fix(services/gcs): Fix content encoding can't be used alone by @Xuanwo in https://github.com/apache/opendal/pull/5614
* fix: ghac doesn't support delete anymore by @Xuanwo in https://github.com/apache/opendal/pull/5628
* fix(services/gdrive): skip the trailing slash when creating and querying the directory by @meteorgan in https://github.com/apache/opendal/pull/5631
### Docs
* docs(bindings/ruby): add documentation for Ruby binding by @erickguan in https://github.com/apache/opendal/pull/5629
* docs: Add upgrade docs for upcoming 0.52 by @Xuanwo in https://github.com/apache/opendal/pull/5634
### CI
* ci: Fix bad corepack cannot find matching keyid by @Xuanwo in https://github.com/apache/opendal/pull/5603
* ci(website): avoid including rc when calculate the latest version by @tisonkun in https://github.com/apache/opendal/pull/5608
* build: upgrade opentelemetry dependencies to 0.28.0 by @tisonkun in https://github.com/apache/opendal/pull/5625
### Chore
* chore(deps): bump uuid from 1.11.0 to 1.12.1 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/5589
* chore(deps): bump uuid from 1.11.0 to 1.12.1 in /core by @dependabot in https://github.com/apache/opendal/pull/5588
* chore(deps): bump log from 0.4.22 to 0.4.25 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/5590
* chore(deps): bump tempfile from 3.15.0 to 3.16.0 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/5586
* chore(deps): update libtest-mimic requirement from 0.7.3 to 0.8.1 in /integrations/object_store by @dependabot in https://github.com/apache/opendal/pull/5587
* chore(layers/prometheus-client): upgrade prometheus-client dependency to v0.23.1 by @koushiro in https://github.com/apache/opendal/pull/5576
* chore(ci): remove benchmark report by @dqhl76 in https://github.com/apache/opendal/pull/5626

## [v0.51.2] - 2025-02-02
### Added
* feat(core): implement if_modified_since and if_unmodified_since for stat_with by @meteorgan in https://github.com/apache/opendal/pull/5528
* feat(layer/otelmetrics): add OtelMetricsLayer by @andylokandy in https://github.com/apache/opendal/pull/5524
* feat(integrations/object_store): implement put_opts and get_opts by @meteorgan in https://github.com/apache/opendal/pull/5513
* feat: Conditional reader for azblob, gcs, oss by @geetanshjuneja in https://github.com/apache/opendal/pull/5531
* feat(core): Add correctness check for read with if_xxx headers by @Xuanwo in https://github.com/apache/opendal/pull/5538
* feat(services/cos): Added user metadata support for cos service by @geetanshjuneja in https://github.com/apache/opendal/pull/5510
* feat(core): Implement list with deleted and versions for oss by @hoslo in https://github.com/apache/opendal/pull/5527
* feat(layer/otelmetrics): take meter when register by @andylokandy in https://github.com/apache/opendal/pull/5547
* feat(gcs): Convert TOO_MANY_REQUESTS to retryable Ratelimited by @Xuanwo in https://github.com/apache/opendal/pull/5551
* feat(services/webdfs): Add user.name support for webhdfs by @Xuanwo in https://github.com/apache/opendal/pull/5567
* feat: disable backtrace for NotFound error by @xxchan in https://github.com/apache/opendal/pull/5577
### Changed
* refactor: refactor some unnecessary clone and use next_back to make clippy happy by @yihong0618 in https://github.com/apache/opendal/pull/5554
* refactor: refactor all body.copy_to_bytes(body.remaining()) by @yihong0618 in https://github.com/apache/opendal/pull/5561
### Fixed
* fix(integrations/object_store) `object_store_opendal` now compiles on wasm32-unknown-unknown by @XiangpengHao in https://github.com/apache/opendal/pull/5530
* fix(serivces/gcs): Gcs doesn't support read with if_(un)modified_since by @Xuanwo in https://github.com/apache/opendal/pull/5537
* fix(logging): remove additional space by @xxchan in https://github.com/apache/opendal/pull/5568
### Docs
* docs: Fix opendal rust core's README not align with new vision by @Xuanwo in https://github.com/apache/opendal/pull/5541
* docs(integration/object_store): add example for datafusion by @meteorgan in https://github.com/apache/opendal/pull/5543
* docs: Add docs on how to pronounce opendal by @Xuanwo in https://github.com/apache/opendal/pull/5552
* docs(bindings/java): better javadoc  by @tisonkun in https://github.com/apache/opendal/pull/5572
### CI
* ci(integration/object_store): add integration tests for object_store_opendal by @meteorgan in https://github.com/apache/opendal/pull/5536
* ci: Pin the nightly version to rust 1.84 for fuzz by @Xuanwo in https://github.com/apache/opendal/pull/5546
* ci: skip running behavior tests when adding or modifying documentation by @meteorgan in https://github.com/apache/opendal/pull/5558
* build: fix Cargo.lock and pass --locked in CI by @xxchan in https://github.com/apache/opendal/pull/5565
* build: implement release process in odev by @tisonkun in https://github.com/apache/opendal/pull/5592
### Chore
* chore: Update CODEOWNERS by @Xuanwo in https://github.com/apache/opendal/pull/5542
* chore(layer/otelmetrics): take meter by reference by @andylokandy in https://github.com/apache/opendal/pull/5553
* chore(core): Avoid using mongodb 3.2.0 by @Xuanwo in https://github.com/apache/opendal/pull/5560
* chore: add oli/oay/ofs to rust-analyzer.linkedProjects by @xxchan in https://github.com/apache/opendal/pull/5564
* chore: try use logforth  by @tisonkun in https://github.com/apache/opendal/pull/5573
* chore: bump version 0.51.2  by @tisonkun in https://github.com/apache/opendal/pull/5595

## [v0.51.1] - 2025-01-08

### Added
* feat(bin/oli): implement oli bench by @tisonkun in https://github.com/apache/opendal/pull/5443
* feat(dev): Add config parse and generate support by @Xuanwo in https://github.com/apache/opendal/pull/5454
* feat(bindings/python): generate python operator constructor types by @trim21 in https://github.com/apache/opendal/pull/5457
* feat(dev): Parse comments from config by @Xuanwo in https://github.com/apache/opendal/pull/5467
* feat(services/core): Implement stat_has_* and list_has_* correctly for services by @geetanshjuneja in https://github.com/apache/opendal/pull/5472
* feat: Add if-match & if-none-match support for reader by @XmchxUp in https://github.com/apache/opendal/pull/5492
* feat(core): Add is_current to metadata by @Wenbin1002 in https://github.com/apache/opendal/pull/5493
* feat(core): Implement list with deleted for s3 service by @Xuanwo in https://github.com/apache/opendal/pull/5498
* feat: generate java configs by @tisonkun in https://github.com/apache/opendal/pull/5503
* feat: Return hinted error for S3 wildcard if-none-match by @gruuya in https://github.com/apache/opendal/pull/5506
* feat(core): implement if_modified_since and if_unmodified_since for read_with and reader_with by @meteorgan in https://github.com/apache/opendal/pull/5500
* feat(core): Implement list with deleted and versions for cos by @hoslo in https://github.com/apache/opendal/pull/5514
### Changed
* refactor: tidy up oli build by @tisonkun in https://github.com/apache/opendal/pull/5438
* refactor(core): Deprecate OpList::version and add versions instead by @geetanshjuneja in https://github.com/apache/opendal/pull/5481
* refactor(dev): use minijinja by @tisonkun in https://github.com/apache/opendal/pull/5494
### Fixed
* fix: exception name in python by @trim21 in https://github.com/apache/opendal/pull/5453
* fix rust warning in python binding by @trim21 in https://github.com/apache/opendal/pull/5459
* fix: python binding kwargs parsing by @trim21 in https://github.com/apache/opendal/pull/5458
* fix(bindings/python): add py.typed marker file by @trim21 in https://github.com/apache/opendal/pull/5464
* fix(services/ghac): Fix stat_with_if_none_match been set in wrong by @Xuanwo in https://github.com/apache/opendal/pull/5477
* fix(ci): Correctly upgrade upload-artifact to v4 by @Xuanwo in https://github.com/apache/opendal/pull/5484
* fix(integration/object_store): object_store requires metadata in list by @Xuanwo in https://github.com/apache/opendal/pull/5501
* fix(services/s3): List with deleted should contain latest by @Xuanwo in https://github.com/apache/opendal/pull/5518
### Docs
* docs: Fix links to vision by @Xuanwo in https://github.com/apache/opendal/pull/5466
* docs(golang): remove unused pkg by @fyqtian in https://github.com/apache/opendal/pull/5473
* docs(core): Polish API docs for `Metadata` by @Xuanwo in https://github.com/apache/opendal/pull/5497
* docs: Polish docs for Operator, Reader and Writer by @Xuanwo in https://github.com/apache/opendal/pull/5516
* docs: Reorganize docs for xxx_with for better reading by @Xuanwo in https://github.com/apache/opendal/pull/5517
### CI
* ci: disable windows free-thread build by @trim21 in https://github.com/apache/opendal/pull/5449
* ci: Upgrade and fix typos by @Xuanwo in https://github.com/apache/opendal/pull/5468
### Chore
* chore(dev): Try just instead of xtasks methods by @Xuanwo in https://github.com/apache/opendal/pull/5461
* chore: pretty gen javadoc by @tisonkun in https://github.com/apache/opendal/pull/5508
* chore(ci): upgrade to manylinux_2_28 for aarch64 Python wheels by @messense in https://github.com/apache/opendal/pull/5522

## [v0.51.0] - 2024-12-14

### Added
* feat(adapter/kv): support async iterating on scan results by @PragmaTwice in https://github.com/apache/opendal/pull/5208
* feat(bindings/ruby): Add simple operators to Ruby binding by @erickguan in https://github.com/apache/opendal/pull/5246
* feat(core/services-gcs): support user defined metadata by @jorgehermo9 in https://github.com/apache/opendal/pull/5276
* feat(core): add `if_not_exist` in `OpWrite` by @kemingy in https://github.com/apache/opendal/pull/5305
* feat: Add {stat,list}_has_* to carry the metadata that backend returns by @Xuanwo in https://github.com/apache/opendal/pull/5318
* feat(core): Implement write if not exists for azblob,azdls,gcs,oss,cos by @Xuanwo in https://github.com/apache/opendal/pull/5321
* feat(core): add new cap shared by @TennyZhuang in https://github.com/apache/opendal/pull/5328
* feat(bindings/python): support pickle [de]serialization for Operator by @TennyZhuang in https://github.com/apache/opendal/pull/5324
* feat(bindings/cpp): init the async support of C++ binding by @PragmaTwice in https://github.com/apache/opendal/pull/5195
* feat(bindings/go): support darwin by @yuchanns in https://github.com/apache/opendal/pull/5334
* feat(services/gdrive): List shows modified timestamp gdrive by @erickguan in https://github.com/apache/opendal/pull/5226
* feat(service/s3): support delete with version by @Frank-III in https://github.com/apache/opendal/pull/5349
* feat: upgrade pyo3 to 0.23 by @XmchxUp in https://github.com/apache/opendal/pull/5368
* feat:  publish python3.13t free-threaded wheel by @XmchxUp in https://github.com/apache/opendal/pull/5387
* feat: add progress bar for oli cp command by @waynexia in https://github.com/apache/opendal/pull/5369
* feat(types/buffer): skip copying in `to_bytes` when `NonContiguous` contains a single `Bytes` by @ever0de in https://github.com/apache/opendal/pull/5388
* feat(bin/oli): support command mv by @meteorgan in https://github.com/apache/opendal/pull/5370
* feat(core): add if-match to `OpWrite` by @Frank-III in https://github.com/apache/opendal/pull/5360
* feat(core/layers): add correctness_check and capability_check layer to verify whether the operation and arguments is supported by @meteorgan in https://github.com/apache/opendal/pull/5352
* feat(bindings/ruby): Add I/O class for Ruby by @erickguan in https://github.com/apache/opendal/pull/5354
* feat(core): Add `content_encoding` to `MetaData` by @Frank-III in https://github.com/apache/opendal/pull/5400
* feat:(core): add `content encoding` to `Opwrite` by @Frank-III in https://github.com/apache/opendal/pull/5390
* feat(services/obs): support user defined metadata by @Frank-III in https://github.com/apache/opendal/pull/5405
* feat: impl configurable OperatorOutputStream maxBytes by @tisonkun in https://github.com/apache/opendal/pull/5422
### Changed
* refactor (bindings/zig): Improvements by @kassane in https://github.com/apache/opendal/pull/5247
* refactor: Remove metakey concept by @Xuanwo in https://github.com/apache/opendal/pull/5319
* refactor(core)!: Remove not used cap write_multi_align_size by @Xuanwo in https://github.com/apache/opendal/pull/5322
* refactor(core)!: Remove the range writer that has never been used by @Xuanwo in https://github.com/apache/opendal/pull/5323
* refactor(core): MaybeSend does not need to be unsafe by @drmingdrmer in https://github.com/apache/opendal/pull/5338
* refactor: Implement RFC-3911 Deleter API  by @Xuanwo in https://github.com/apache/opendal/pull/5392
* refactor: Remove batch concept from opendal by @Xuanwo in https://github.com/apache/opendal/pull/5393
### Fixed
* fix(services/webdav): Fix lister failing when root contains spaces by @skrimix in https://github.com/apache/opendal/pull/5298
* fix(bindings/c): Bump min CMake version to support CMP0135 by @palash25 in https://github.com/apache/opendal/pull/5308
* fix(services/webhdfs): rename auth value by @notauserx in https://github.com/apache/opendal/pull/5342
* fix(bindings/cpp): remove the warning of CMP0135 by @PragmaTwice in https://github.com/apache/opendal/pull/5346
* build(python): fix pyproject meta file by @trim21 in https://github.com/apache/opendal/pull/5348
* fix(services/unftp): add `/` when not presented by @Frank-III in https://github.com/apache/opendal/pull/5382
* fix: update document against target format check and add hints by @waynexia in https://github.com/apache/opendal/pull/5361
* fix: oli clippy and CI file by @waynexia in https://github.com/apache/opendal/pull/5389
* fix(services/obs): support huawei.com by @FayeSpica in https://github.com/apache/opendal/pull/5399
* fix(integrations/cloud_filter): use explicit `stat` instead of `Entry::metadata` in `fetch_placeholders` by @ho-229 in https://github.com/apache/opendal/pull/5416
* fix(core): S3 multipart uploads does not set file metadata by @catcatmu in https://github.com/apache/opendal/pull/5430
* fix: always contains path label if configured by @waynexia in https://github.com/apache/opendal/pull/5433
### Docs
* docs: Enable force_orphan to reduce clone size by @Xuanwo in https://github.com/apache/opendal/pull/5289
* docs: Establish VISION for "One Layer, All Storage" by @Xuanwo in https://github.com/apache/opendal/pull/5309
* docs: Polish docs for write with if not exists by @Xuanwo in https://github.com/apache/opendal/pull/5320
* docs(core): add the description of version parameter for operator by @meteorgan in https://github.com/apache/opendal/pull/5144
* docs(core): Add upgrade to v0.51 by @Xuanwo in https://github.com/apache/opendal/pull/5406
* docs: Update release.md by @tisonkun in https://github.com/apache/opendal/pull/5431
### CI
* ci: Remove the token of codspeed by @Xuanwo in https://github.com/apache/opendal/pull/5283
* ci: Allow force push for `gh-pages` by @Xuanwo in https://github.com/apache/opendal/pull/5290
* build(bindings/java): fix lombok process by @tisonkun in https://github.com/apache/opendal/pull/5297
* build(bindings/python): add python 3.10 back and remove pypy by @trim21 in https://github.com/apache/opendal/pull/5347
### Chore
* chore(core/layers): align `info` method of `trait Access` and `trait LayeredAccess` by @koushiro in https://github.com/apache/opendal/pull/5258
* chore(core/raw): align `info` method of `kv::Adapter` and `typed_kv::Adapter` by @koushiro in https://github.com/apache/opendal/pull/5259
* chore(layers/oteltrace): adjust tracer span name of info method by @koushiro in https://github.com/apache/opendal/pull/5285
* chore(services/s3): remove versioning check for s3 by @meteorgan in https://github.com/apache/opendal/pull/5300
* chore: Polish the debug output of capability by @Xuanwo in https://github.com/apache/opendal/pull/5315
* chore: Update maturity.md by @tisonkun in https://github.com/apache/opendal/pull/5340
* chore: remove flagset in cargo.lock by @meteorgan in https://github.com/apache/opendal/pull/5355
* chore: add setup action for hadoop to avoid build failures by @meteorgan in https://github.com/apache/opendal/pull/5353
* chore: fix cargo clippy by @meteorgan in https://github.com/apache/opendal/pull/5379
* chore: fix cargo clippy by @meteorgan in https://github.com/apache/opendal/pull/5384
* chore: fix Bindings OCaml CI  by @meteorgan in https://github.com/apache/opendal/pull/5386
* chore: Add default vscode config for more friendly developer experience by @Zheaoli in https://github.com/apache/opendal/pull/5331
* chore(website): remove outdated description by @meteorgan in https://github.com/apache/opendal/pull/5411
* chore(deps): bump clap from 4.5.20 to 4.5.21 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/5372
* chore(deps): bump anyhow from 1.0.90 to 1.0.93 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/5375
* chore(deps): bump serde from 1.0.210 to 1.0.215 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/5376
* chore(deps): bump openssh-sftp-client from 0.15.1 to 0.15.2 in /core by @dependabot in https://github.com/apache/opendal/pull/5377
* chore(ci): fix invalid Behavior Test Integration Cloud Filter trigger by @Zheaoli in https://github.com/apache/opendal/pull/5414

## [v0.50.2] - 2024-11-04

### Added
* feat(services/ftp): List dir shows last modified timestamp by @erickguan in https://github.com/apache/opendal/pull/5213
* feat(bindings/d): add D bindings support by @kassane in https://github.com/apache/opendal/pull/5181
* feat(bindings/python): add sync `File.readline` by @TennyZhuang in https://github.com/apache/opendal/pull/5271
* feat(core/services-azblob): support user defined metadata by @jorgehermo9 in https://github.com/apache/opendal/pull/5274
* feat(core/services-s3): try load endpoint from config by @TennyZhuang in https://github.com/apache/opendal/pull/5279
### Changed
* refactor(bin/oli): use `clap_derive` to reduce boilerplate code by @koushiro in https://github.com/apache/opendal/pull/5233
### Fixed
* fix: add all-features flag for opendal_compat doc build by @XmchxUp in https://github.com/apache/opendal/pull/5234
* fix(integrations/compat): Capability has different fields by @Xuanwo in https://github.com/apache/opendal/pull/5236
* fix(integration/compat): Fix opendal 0.50 OpList has new field by @Xuanwo in https://github.com/apache/opendal/pull/5238
* fix(integrations/compat): Fix dead loop happened during list by @Xuanwo in https://github.com/apache/opendal/pull/5240
### Docs
* docs: Move our release process to github discussions by @Xuanwo in https://github.com/apache/opendal/pull/5217
* docs: change "Github" to "GitHub" by @MohammadLotfiA in https://github.com/apache/opendal/pull/5250
### CI
* ci(asf): Don't add `[DISCUSS]` prefix for discussion by @Xuanwo in https://github.com/apache/opendal/pull/5210
* build: enable services-mysql for Java and Python bindings by @tisonkun in https://github.com/apache/opendal/pull/5222
* build(binding/python): Support Python 3.13 by @Zheaoli in https://github.com/apache/opendal/pull/5248
### Chore
* chore(bin/*): remove useless deps by @koushiro in https://github.com/apache/opendal/pull/5212
* chore: tidy up c binding build and docs by @tisonkun in https://github.com/apache/opendal/pull/5243
* chore(core/layers): adjust await point to simplify combinator code by @koushiro in https://github.com/apache/opendal/pull/5255
* chore(core/blocking_operator): deduplicate deprecated `is_exist` logic by @simonsan in https://github.com/apache/opendal/pull/5261
* chore(deps): bump actions/cache from 3 to 4 by @dependabot in https://github.com/apache/opendal/pull/5262
* chore: run object_store tests in CI by @jorgehermo9 in https://github.com/apache/opendal/pull/5268

## [v0.50.1] - 2024-10-20

### Added
* feat(core/redis): Replace client requests with connection pool by @jackyyyyyssss in https://github.com/apache/opendal/pull/5117
* feat: add copy api for lakefs service.  by @liugddx in https://github.com/apache/opendal/pull/5114
* feat(core): add version(bool) for List operation to include version d… by @meteorgan in https://github.com/apache/opendal/pull/5106
* feat(bindings/python): export ConcurrentLimitLayer by @TennyZhuang in https://github.com/apache/opendal/pull/5140
* feat(bindings/c): add writer operation for Bindings C and Go by @yuchanns in https://github.com/apache/opendal/pull/5141
* feat(ofs): introduce ofs macos support by @oowl in https://github.com/apache/opendal/pull/5136
* feat: Reduce stat operation if we are reading all by @Xuanwo in https://github.com/apache/opendal/pull/5146
* feat: add NebulaGraph config by @GG2002 in https://github.com/apache/opendal/pull/5147
* feat(integrations/spring): add spring serialize method by @shoothzj in https://github.com/apache/opendal/pull/5154
* feat: support write,read,delete with template by @shoothzj in https://github.com/apache/opendal/pull/5156
* feat(bindings/java): support ConcurrentLimitLayer by @tisonkun in https://github.com/apache/opendal/pull/5168
* feat: Add if_none_match for write by @ForestLH in https://github.com/apache/opendal/pull/5129
* feat: Add OpenDAL Compat by @Xuanwo in https://github.com/apache/opendal/pull/5185
* feat(core): abstract HttpFetch trait for raw http client by @everpcpc in https://github.com/apache/opendal/pull/5184
* feat: Support NebulaGraph by @GG2002 in https://github.com/apache/opendal/pull/5116
* feat(bindings/cpp): rename is_exist to exists as core did by @PragmaTwice in https://github.com/apache/opendal/pull/5198
* feat(bindings/c): add opendal_operator_exists and mark is_exist deprecated by @PragmaTwice in https://github.com/apache/opendal/pull/5199
* feat(binding/java): prefix thread name with opendal-tokio-worker by @tisonkun in https://github.com/apache/opendal/pull/5197
### Changed
* refactor(services/cloudflare-kv): remove unneeded async and result on parse_error by @tsfotis in https://github.com/apache/opendal/pull/5128
* refactor(*): remove unneeded async and result on parse_error by @tsfotis in https://github.com/apache/opendal/pull/5131
* refactor: align C binding pattern by @tisonkun in https://github.com/apache/opendal/pull/5160
* refactor: more consistent C binding pattern by @tisonkun in https://github.com/apache/opendal/pull/5162
* refactor(integration/parquet): Use ParquetMetaDataReader instead by @Xuanwo in https://github.com/apache/opendal/pull/5170
* refactor: resolve c pointers const by @tisonkun in https://github.com/apache/opendal/pull/5171
* refactor(types/operator): rename is_exist to exists by @photino in https://github.com/apache/opendal/pull/5193
### Fixed
* fix(services/huggingface): Align with latest HuggingFace API by @morristai in https://github.com/apache/opendal/pull/5123
* fix(bindings/c): use `ManuallyDrop` instead of `forget` to make sure pointer is valid by @ethe in https://github.com/apache/opendal/pull/5166
* fix(services/s3): Mark xml deserialize error as temporary during list by @Xuanwo in https://github.com/apache/opendal/pull/5178
### Docs
* docs: add spring integration configuration doc by @shoothzj in https://github.com/apache/opendal/pull/5053
* docs: improve Node.js binding's test doc by @tisonkun in https://github.com/apache/opendal/pull/5159
* docs(bindings/c): update docs for CMake replacing by @PragmaTwice in https://github.com/apache/opendal/pull/5186
### CI
* ci(bindings/nodejs): Fix diff introduced by napi by @Xuanwo in https://github.com/apache/opendal/pull/5121
* ci: Disable aliyun drive test until #5163 addressed by @Xuanwo in https://github.com/apache/opendal/pull/5164
* ci: add package cache for build-haskell-doc by @XmchxUp in https://github.com/apache/opendal/pull/5173
* ci: add cache action for ci_bindings_ocaml & build-ocaml-doc by @XmchxUp in https://github.com/apache/opendal/pull/5174
* ci: Fix failing CI on ocaml and python by @Xuanwo in https://github.com/apache/opendal/pull/5177
* build(bindings/c): replace the build system with CMake by @PragmaTwice in https://github.com/apache/opendal/pull/5182
* build(bindings/cpp): fetch and build dependencies instead of finding system libs by @PragmaTwice in https://github.com/apache/opendal/pull/5188
* ci: Remove not needed --break-system-packages by @Xuanwo in https://github.com/apache/opendal/pull/5196
* ci: Send discussions to dev@o.a.o by @Xuanwo in https://github.com/apache/opendal/pull/5201
### Chore
* chore(bindings/python): deprecate via_map method by @TennyZhuang in https://github.com/apache/opendal/pull/5134
* chore: update binding java artifact name in README by @tisonkun in https://github.com/apache/opendal/pull/5137
* chore(fixtures/s3): Upgrade MinIO version by @ForestLH in https://github.com/apache/opendal/pull/5142
* chore(deps): bump clap from 4.5.17 to 4.5.18 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/5149
* chore(deps): bump crate-ci/typos from 1.24.3 to 1.24.6 by @dependabot in https://github.com/apache/opendal/pull/5150
* chore(deps): bump anyhow from 1.0.87 to 1.0.89 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/5151
* chore(deps): bump anyhow from 1.0.87 to 1.0.89 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/5152
* chore: fix typos in tokio_executor.rs by @tisonkun in https://github.com/apache/opendal/pull/5157
* chore: hint when java tests are skipped by @tisonkun in https://github.com/apache/opendal/pull/5158
* chore: Include license in the packaged crate by @davide125 in https://github.com/apache/opendal/pull/5176

## [v0.50.0] - 2024-09-11

### Added
* feat(core)!: make list return path itself by @meteorgan in https://github.com/apache/opendal/pull/4959
* feat(services/oss): support role_arn and oidc_provider_arn by @tisonkun in https://github.com/apache/opendal/pull/5063
* feat(services): add lakefs support by @liugddx in https://github.com/apache/opendal/pull/5086
* feat: add list api for lakefs service. by @liugddx in https://github.com/apache/opendal/pull/5092
* feat: add write api for lakefs service. by @liugddx in https://github.com/apache/opendal/pull/5100
* feat: add delete api for lakefs service. by @liugddx in https://github.com/apache/opendal/pull/5107
### Changed
* refactor: use sqlx for sql services  by @tisonkun in https://github.com/apache/opendal/pull/5040
* refactor(core)!: Add observe layer as building block by @Xuanwo in https://github.com/apache/opendal/pull/5064
* refactor(layers/prometheus): rewrite prometheus layer based on observe mod by @koushiro in https://github.com/apache/opendal/pull/5069
* refactor(bindings/java): replace `num_cpus` with `std::thread::available_parallelism` by @miroim in https://github.com/apache/opendal/pull/5080
* refactor(layers/prometheus): provide builder APIs by @koushiro in https://github.com/apache/opendal/pull/5072
* refactor(layers/prometheus-client): provide builder APIs by @koushiro in https://github.com/apache/opendal/pull/5073
* refactor(layers/metrics): rewrite metrics layer using observe layer by @koushiro in https://github.com/apache/opendal/pull/5098
### Fixed
* fix(core): TimeoutLayer now needs enable tokio time by @Xuanwo in https://github.com/apache/opendal/pull/5057
* fix(core): Fix failed list related tests by @Xuanwo in https://github.com/apache/opendal/pull/5058
* fix(services/memory): blocking_scan right range by @meteorgan in https://github.com/apache/opendal/pull/5062
* fix(core/services/mysql): Fix mysql Capability by @jackyyyyyssss in https://github.com/apache/opendal/pull/5067
* fix: fix rust 1.76 error due to temporary value being dropped by @aawsome in https://github.com/apache/opendal/pull/5071
* fix(service/fs): error due to temporary value being dropped by @miroim in https://github.com/apache/opendal/pull/5079
* fix(core/services/hdfs): Fix the HDFS write failure when atomic_write_dir is set by @meteorgan in https://github.com/apache/opendal/pull/5039
* fix(services/icloud): adjust error handling code to avoid having to write out result type explicitly by @koushiro in https://github.com/apache/opendal/pull/5091
* fix(services/monoiofs): handle async cancel during file open by @NKID00 in https://github.com/apache/opendal/pull/5094
### Docs
* docs: Update binding-java.md by @tisonkun in https://github.com/apache/opendal/pull/5087
### CI
* ci(bindings/go): add golangci-lint by @yuchanns in https://github.com/apache/opendal/pull/5060
* ci(bindings/zig): Fix build and test of zig on 0.13 by @Xuanwo in https://github.com/apache/opendal/pull/5102
* ci: Don't publish with all features by @Xuanwo in https://github.com/apache/opendal/pull/5108
* ci: Fix upload-artifacts doesn't include hidden files by @Xuanwo in https://github.com/apache/opendal/pull/5112
### Chore
* chore(bindings/go): bump ffi and sys version by @shoothzj in https://github.com/apache/opendal/pull/5055
* chore: Bump backon to 1.0.0 by @Xuanwo in https://github.com/apache/opendal/pull/5056
* chore(services/rocksdb): fix misuse rocksdb prefix iterator by @meteorgan in https://github.com/apache/opendal/pull/5059
* chore(README): add Go binding badge by @yuchanns in https://github.com/apache/opendal/pull/5074
* chore(deps): bump crate-ci/typos from 1.23.6 to 1.24.3 by @dependabot in https://github.com/apache/opendal/pull/5085
* chore(layers/prometheus-client): export `PrometheusClientLayerBuilder` type by @koushiro in https://github.com/apache/opendal/pull/5093
* chore(layers): check the examples when running tests by @koushiro in https://github.com/apache/opendal/pull/5104
* chore(integrations/parquet): Bump parquet to 53 by @Xuanwo in https://github.com/apache/opendal/pull/5109
* chore: Bump OpenDAL to 0.50.0 by @Xuanwo in https://github.com/apache/opendal/pull/5110

## [v0.49.2] - 2024-08-26

### Added
* feat(ovfs): support read and write by @zjregee in https://github.com/apache/opendal/pull/5016
* feat(bin/ofs): introduce `integrations/cloudfilter` for ofs by @ho-229 in https://github.com/apache/opendal/pull/4935
* feat(integrations/spring): add AutoConfiguration class for Spring Mvc and Webflux by @shoothzj in https://github.com/apache/opendal/pull/5019
* feat(services/monoiofs): impl read and write, add behavior test by @NKID00 in https://github.com/apache/opendal/pull/4944
* feat(core/services-s3): support user defined metadata by @haoqixu in https://github.com/apache/opendal/pull/5030
* feat: align `fn root` semantics; fix missing root for some services; rm duplicated normalize ops by @yjhmelody in https://github.com/apache/opendal/pull/5035
* feat(core): expose configs always by @tisonkun in https://github.com/apache/opendal/pull/5034
* feat(services/monoiofs): append, create_dir, copy and rename by @NKID00 in https://github.com/apache/opendal/pull/5041
### Changed
* refactor(core): new type to print context and reduce allocations by @evenyag in https://github.com/apache/opendal/pull/5021
* refactor(layers/prometheus-client): remove useless `scheme` field from `PrometheusAccessor` and `PrometheusMetricWrapper` type by @koushiro in https://github.com/apache/opendal/pull/5026
* refactor(layers/prometheus-client): avoid multiple clone of labels by @koushiro in https://github.com/apache/opendal/pull/5028
* refactor(core/services-oss): remove the `starts_with` by @haoqixu in https://github.com/apache/opendal/pull/5036
### Fixed
* fix(layers/prometheus-client): remove duplicated `increment_request_total` of write operation by @koushiro in https://github.com/apache/opendal/pull/5023
* fix(services/monoiofs): drop JoinHandle in worker thread by @NKID00 in https://github.com/apache/opendal/pull/5031
### CI
* ci: Add contents write permission for build-website by @Xuanwo in https://github.com/apache/opendal/pull/5017
* ci: Fix test for service ghac by @Xuanwo in https://github.com/apache/opendal/pull/5018
* ci(integrations/spring): add spring boot bean load test by @shoothzj in https://github.com/apache/opendal/pull/5032
### Chore
* chore: fix path typo in release docs by @tisonkun in https://github.com/apache/opendal/pull/5038
* chore: align the `token` method semantics by @yjhmelody in https://github.com/apache/opendal/pull/5045

## [v0.49.1] - 2024-08-15

### Added
* feat(ovfs): add lookup and unit tests by @zjregee in https://github.com/apache/opendal/pull/4997
* feat(gcs): allow setting a token directly by @jdockerty in https://github.com/apache/opendal/pull/4978
* feat(integrations/cloudfilter): introduce behavior tests by @ho-229 in https://github.com/apache/opendal/pull/4973
* feat(integrations/spring): add spring project module by @shoothzj in https://github.com/apache/opendal/pull/4988
* feat(fs): expose the metadata for fs services by @Aitozi in https://github.com/apache/opendal/pull/5005
* feat(ovfs): add file creation and deletion by @zjregee in https://github.com/apache/opendal/pull/5009
### Fixed
* fix(integrations/spring): correct parent artifactId by @shoothzj in https://github.com/apache/opendal/pull/5007
* fix(bindings/python): Make sure read until EOF by @Bicheka in https://github.com/apache/opendal/pull/4995
### Docs
* docs: Fix version detect in website by @Xuanwo in https://github.com/apache/opendal/pull/5003
* docs: add branding, license and trademarks to integrations by @PsiACE in https://github.com/apache/opendal/pull/5006
* docs(integrations/cloudfilter): improve docs and examples by @ho-229 in https://github.com/apache/opendal/pull/5010
### CI
* ci(bindings/python): Fix aws-lc-rs build on arm platforms by @Xuanwo in https://github.com/apache/opendal/pull/5004
### Chore
* chore(deps): bump fastrace to 0.7.1 by @andylokandy in https://github.com/apache/opendal/pull/5008
* chore(bindings): Disable mysql service for java and python by @Xuanwo in https://github.com/apache/opendal/pull/5013

## [v0.49.0] - 2024-08-09

### Added
* feat(o): Add cargo-o layout by @Xuanwo in https://github.com/apache/opendal/pull/4934
* feat: impl `put_multipart` in `object_store` by @Rachelint in https://github.com/apache/opendal/pull/4793
* feat: introduce opendal `AsyncWriter` for parquet integrations  by @WenyXu in https://github.com/apache/opendal/pull/4958
* feat(services/http): implement presigned request for backends without authorization by @NickCao in https://github.com/apache/opendal/pull/4970
* feat(bindings/python): strip the library for minimum file size by @NickCao in https://github.com/apache/opendal/pull/4971
* feat(gcs): allow unauthenticated requests by @jdockerty in https://github.com/apache/opendal/pull/4965
* feat: introduce opendal `AsyncReader` for parquet integrations by @WenyXu in https://github.com/apache/opendal/pull/4972
* feat(services/s3): add role_session_name in assume roles by @nerdroychan in https://github.com/apache/opendal/pull/4981
* feat: support root path for moka and mini-moka by @meteorgan in https://github.com/apache/opendal/pull/4984
* feat(ovfs): export VirtioFs struct by @zjregee in https://github.com/apache/opendal/pull/4983
* feat(core)!: implement an interceptor for the logging layer by @evenyag in https://github.com/apache/opendal/pull/4961
* feat(ovfs): support getattr and setattr by @zjregee in https://github.com/apache/opendal/pull/4987
### Changed
* refactor(java)!: Rename artifacts id opendal-java to opendal by @Xuanwo in https://github.com/apache/opendal/pull/4957
* refactor(core)!: Return associated builder instead by @Xuanwo in https://github.com/apache/opendal/pull/4968
* refactor(raw): Merge all operations into one enum by @Xuanwo in https://github.com/apache/opendal/pull/4977
* refactor(core): Use kv based context to avoid allocations by @Xuanwo in https://github.com/apache/opendal/pull/4986
### Fixed
* fix(services/memory): MemoryConfig implement Debug by @0x676e67 in https://github.com/apache/opendal/pull/4942
* fix(layers/promethues-client): doc link by @koushiro in https://github.com/apache/opendal/pull/4951
* fix(gcs): do not skip signing with `allow_anonymous` by @jdockerty in https://github.com/apache/opendal/pull/4979
### Docs
* docs: nominate-committer add announcement template by @tisonkun in https://github.com/apache/opendal/pull/4954
### CI
* ci: Bump nextest to 0.9.72 by @Xuanwo in https://github.com/apache/opendal/pull/4932
* ci: setup cloudfilter by @ho-229 in https://github.com/apache/opendal/pull/4936
* ci: Try fix opendal-lua build by @Xuanwo in https://github.com/apache/opendal/pull/4952
### Chore
* chore(deps): bump crate-ci/typos from 1.22.9 to 1.23.6 by @dependabot in https://github.com/apache/opendal/pull/4948
* chore(deps): bump tokio from 1.39.1 to 1.39.2 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/4949
* chore(deps): bump bytes from 1.6.1 to 1.7.0 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/4947
* chore(deps): bump tokio from 1.39.1 to 1.39.2 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/4946
* chore(core): fix nightly lints by @xxchan in https://github.com/apache/opendal/pull/4953
* chore(integrations/parquet): add README by @WenyXu in https://github.com/apache/opendal/pull/4980
* chore(core): Bump redis version by @Xuanwo in https://github.com/apache/opendal/pull/4985
* chore: Bump package versions by @Xuanwo in https://github.com/apache/opendal/pull/4989

## [v0.48.0] - 2024-07-26

### Added
* feat(services/fs): Support fs config by @meteorgan in https://github.com/apache/opendal/pull/4853
* feat(services): init monoiofs by @NKID00 in https://github.com/apache/opendal/pull/4855
* feat(core/types): avoid a copy in `Buffer::to_bytes()` by cloning contiguous bytes by @LDeakin in https://github.com/apache/opendal/pull/4858
* feat(core): Add object versioning for OSS by @Lzzzzzt in https://github.com/apache/opendal/pull/4870
* feat: fs add concurrent write by @hoslo in https://github.com/apache/opendal/pull/4817
* feat(services/s3): Add object versioning for S3 by @Lzzzzzt in https://github.com/apache/opendal/pull/4873
* feat(integrations/cloudfilter): read only cloud filter by @ho-229 in https://github.com/apache/opendal/pull/4856
* feat(bindings/go): Add full native support from C to Go. by @yuchanns in https://github.com/apache/opendal/pull/4886
* feat(bindings/go): add benchmark. by @yuchanns in https://github.com/apache/opendal/pull/4893
* feat(core): support user defined metadata for oss by @meteorgan in https://github.com/apache/opendal/pull/4881
* feat(service/fastrace): rename minitrace to fastrace by @andylokandy in https://github.com/apache/opendal/pull/4906
* feat(prometheus-client): add metric label about `root` on using PrometheusClientLayer by @flaneur2020 in https://github.com/apache/opendal/pull/4907
* feat(services/monoiofs): monoio wrapper by @NKID00 in https://github.com/apache/opendal/pull/4885
* feat(layers/mime-guess): add a layer that can automatically set `Content-Type` based on the extension in the path. by @czy-29 in https://github.com/apache/opendal/pull/4912
* feat(core)!: Make config data object by @tisonkun in https://github.com/apache/opendal/pull/4915
* feat(core)!: from_map is now fallible by @tisonkun in https://github.com/apache/opendal/pull/4917
* ci(bindings/go): always test against the latest core by @yuchanns in https://github.com/apache/opendal/pull/4913
* feat(!): Allow users to build operator from config by @Xuanwo in https://github.com/apache/opendal/pull/4919
* feat: Add from_iter and via_iter for operator by @Xuanwo in https://github.com/apache/opendal/pull/4921
### Changed
* refactor(services/s3)!: renamed security_token to session_token by @Zyyeric in https://github.com/apache/opendal/pull/4875
* refactor(core)!: Make oio::Write always write all given buffer by @Xuanwo in https://github.com/apache/opendal/pull/4880
* refactor(core)!: Return `Arc<AccessInfo>` for metadata by @Lzzzzzt in https://github.com/apache/opendal/pull/4883
* refactor(core!): Make service builder takes ownership by @Xuanwo in https://github.com/apache/opendal/pull/4922
* refactor(integrations/cloudfilter): implement Filter instead of SyncFilter by @ho-229 in https://github.com/apache/opendal/pull/4920
### Fixed
* fix(services/s3): NoSuchBucket is a ConfigInvalid for OpenDAL by @tisonkun in https://github.com/apache/opendal/pull/4895
* fix: oss will not use the port by @Lzzzzzt in https://github.com/apache/opendal/pull/4899
### Docs
* docs(core): update README to add `MimeGuessLayer`. by @czy-29 in https://github.com/apache/opendal/pull/4916
* docs(core): Add upgrade docs for 0.48 by @Xuanwo in https://github.com/apache/opendal/pull/4924
* docs: fix spelling by @jbampton in https://github.com/apache/opendal/pull/4925
* docs(core): Fix comment for into_futures_async_write by @Xuanwo in https://github.com/apache/opendal/pull/4928
### CI
* ci: Add issue template and pr template for opendal by @Xuanwo in https://github.com/apache/opendal/pull/4884
* ci: Remove CI reviewer since it doesn't work by @Xuanwo in https://github.com/apache/opendal/pull/4891
### Chore
* chore!: fix typo customed should be customized by @tisonkun in https://github.com/apache/opendal/pull/4847
* chore: Fix spelling by @jbampton in https://github.com/apache/opendal/pull/4864
* chore: remove unneeded duplicate word by @jbampton in https://github.com/apache/opendal/pull/4865
* chore: fix spelling by @jbampton in https://github.com/apache/opendal/pull/4866
* chore: fix spelling by @NKID00 in https://github.com/apache/opendal/pull/4869
* chore: Make compfs able to test by @Xuanwo in https://github.com/apache/opendal/pull/4878
* chore(services/compfs): remove allow(dead_code) by @George-Miao in https://github.com/apache/opendal/pull/4879
* chore: Make rust 1.80 clippy happy by @Xuanwo in https://github.com/apache/opendal/pull/4927
* chore: Bump crates versions  by @Xuanwo in https://github.com/apache/opendal/pull/4929

## [v0.47.3] - 2024-07-03

### Changed
* refactor: Move ChunkedWrite logic into WriteContext by @Xuanwo in https://github.com/apache/opendal/pull/4826
* refactor(services/aliyun-drive): directly implement `oio::Write`. by @yuchanns in https://github.com/apache/opendal/pull/4821
### Fixed
* fix(integration/object_store): Avoid calling  API inside debug by @Xuanwo in https://github.com/apache/opendal/pull/4846
* fix(integration/object_store): Fix metakey requested is incomplete by @Xuanwo in https://github.com/apache/opendal/pull/4844
### Docs
* docs(integration/unftp-sbe): Polish docs for unftp-sbe by @Xuanwo in https://github.com/apache/opendal/pull/4838
* docs(bin): Polish README for all bin by @Xuanwo in https://github.com/apache/opendal/pull/4839
### Chore
* chore(deps): bump crate-ci/typos from 1.22.7 to 1.22.9 by @dependabot in https://github.com/apache/opendal/pull/4836
* chore(deps): bump quick-xml from 0.32.0 to 0.35.0 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/4835
* chore(deps): bump nix from 0.28.0 to 0.29.0 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/4833
* chore(deps): bump metrics from 0.20.1 to 0.23.0 in /core by @TennyZhuang in https://github.com/apache/opendal/pull/4843

## [v0.47.2] - 2024-06-30

### Added
* feat(services/compfs): basic `Access` impl by @George-Miao in https://github.com/apache/opendal/pull/4693
* feat(unftp-sbe): impl `OpendalStorage` by @George-Miao in https://github.com/apache/opendal/pull/4765
* feat(services/compfs): implement auxiliary functions by @George-Miao in https://github.com/apache/opendal/pull/4778
* feat: make AwaitTreeLayer covers oio::Read and oio::Write by @PsiACE in https://github.com/apache/opendal/pull/4787
* feat: Nodejs add devbox by @bxb100 in https://github.com/apache/opendal/pull/4791
* feat: make AsyncBacktraceLayer covers oio::Read and oio::Write by @PsiACE in https://github.com/apache/opendal/pull/4789
* feat(nodejs): add `WriteOptions` for write methods by @bxb100 in https://github.com/apache/opendal/pull/4785
* feat: setup cloud filter integration by @ho-229 in https://github.com/apache/opendal/pull/4779
* feat: add position write by @hoslo in https://github.com/apache/opendal/pull/4795
* fix(core): write concurrent doesn't set correctly by @hoslo in https://github.com/apache/opendal/pull/4816
* feat(ovfs): add filesystem to handle message by @zjregee in https://github.com/apache/opendal/pull/4720
* feat(unftp-sbe): add derives for `OpendalMetadata` by @George-Miao in https://github.com/apache/opendal/pull/4819
* feat(core/gcs): Add concurrent write for gcs back by @Xuanwo in https://github.com/apache/opendal/pull/4820
### Changed
* refactor(nodejs)!: Remove append api by @bxb100 in https://github.com/apache/opendal/pull/4796
* refactor(core): Remove unused layer `MadsimLayer` by @zzzk1 in https://github.com/apache/opendal/pull/4788
### Fixed
* fix(services/aliyun-drive): list dir without trailing slash by @yuchanns in https://github.com/apache/opendal/pull/4766
* fix(unftp-sbe): remove buffer for get by @George-Miao in https://github.com/apache/opendal/pull/4775
* fix(services/aliyun-drive): write op cannot overwrite existing files by @yuchanns in https://github.com/apache/opendal/pull/4781
* fix(core/services/onedrive): remove @odata.count for onedrive list op by @imWildCat in https://github.com/apache/opendal/pull/4803
* fix(core): Gcs's RangeWrite doesn't support concurrent write by @Xuanwo in https://github.com/apache/opendal/pull/4806
* fix(tests/behavior): skip test of write_with_overwrite for ghac by @yuchanns in https://github.com/apache/opendal/pull/4823
* fix(docs): some typos in website and nodejs binding docs by @suyanhanx in https://github.com/apache/opendal/pull/4814
* fix(core/aliyun_drive): Fix write_multi_max_size might overflow by @Xuanwo in https://github.com/apache/opendal/pull/4830

### Docs
* doc(unftp-sbe): adds example and readme by @George-Miao in https://github.com/apache/opendal/pull/4777
* doc(nodejs): update upgrade.md by @bxb100 in https://github.com/apache/opendal/pull/4799
* docs: Add README and rustdoc for fuse3_opendal by @Xuanwo in https://github.com/apache/opendal/pull/4813
* docs: use version variable in gradle, same to maven by @shoothzj in https://github.com/apache/opendal/pull/4824
### CI
* ci: set behavior test ci for aliyun drive by @suyanhanx in https://github.com/apache/opendal/pull/4657
* ci: Fix lib-darwin-x64 no released by @Xuanwo in https://github.com/apache/opendal/pull/4798
* ci(unftp-sbe): init by @George-Miao in https://github.com/apache/opendal/pull/4809
* ci: Build docs for all integrations by @Xuanwo in https://github.com/apache/opendal/pull/4811
* ci(scripts): Add a script to generate version list by @Xuanwo in https://github.com/apache/opendal/pull/4827
### Chore
* chore(ci): disable aliyun_drive for bindings test by @suyanhanx in https://github.com/apache/opendal/pull/4770
* chore(unftp-sbe): remove Cargo.lock by @George-Miao in https://github.com/apache/opendal/pull/4805

## [v0.47.1] - 2024-06-18

### Added
* feat(core): sets default chunk_size and sends buffer > chunk_size directly by @evenyag in https://github.com/apache/opendal/pull/4710
* feat(services): add optional access_token for AliyunDrive by @yuchanns in https://github.com/apache/opendal/pull/4740
* feat(unftp-sbe): Add integration for unftp-sbe by @George-Miao in https://github.com/apache/opendal/pull/4753
### Changed
* refactor(ofs): Split fuse3 impl into fuse3_opendal  by @Xuanwo in https://github.com/apache/opendal/pull/4721
* refactor(ovfs): Split ovfs impl into virtiofs_opendal by @zjregee in https://github.com/apache/opendal/pull/4723
* refactor(*): tiny refactor to the Error type by @waynexia in https://github.com/apache/opendal/pull/4737
* refactor(aliyun-drive): rewrite writer part by @yuchanns in https://github.com/apache/opendal/pull/4744
* refactor(object_store): Polish implementation details of object_store by @Xuanwo in https://github.com/apache/opendal/pull/4749
* refactor(dav-server): Polish dav-server integration details by @Xuanwo in https://github.com/apache/opendal/pull/4751
* refactor(core): Remove unused `size` for `RangeWrite`. by @reswqa in https://github.com/apache/opendal/pull/4755
### Fixed
* fix(s3): parse MultipartUploadResponse to check error in body by @waynexia in https://github.com/apache/opendal/pull/4735
* fix(services/aliyun-drive): unable to list `/` by @yuchanns in https://github.com/apache/opendal/pull/4754
### Docs
* docs: keep docs updated and tidy by @tisonkun in https://github.com/apache/opendal/pull/4709
* docs: fixup broken links by @tisonkun in https://github.com/apache/opendal/pull/4711
* docs(website): update release/verify docs by @suyanhanx in https://github.com/apache/opendal/pull/4714
* docs: Update release.md link correspondingly by @tisonkun in https://github.com/apache/opendal/pull/4717
* docs: update readme for fuse3_opendal & virtiofs_opendal by @zjregee in https://github.com/apache/opendal/pull/4730
* docs: Polish README and links to docs by @Xuanwo in https://github.com/apache/opendal/pull/4741
* docs: Enhance maintainability of the service section by @Xuanwo in https://github.com/apache/opendal/pull/4742
* docs: Polish opendal rust core README by @Xuanwo in https://github.com/apache/opendal/pull/4745
* docs: Refactor rust core examples by @Xuanwo in https://github.com/apache/opendal/pull/4757
### CI
* ci: verify build website on site content changes by @tisonkun in https://github.com/apache/opendal/pull/4712
* ci: Fix cert for redis and add docs for key maintenance by @Xuanwo in https://github.com/apache/opendal/pull/4718
* ci(nodejs): Disable services-all on windows by @Xuanwo in https://github.com/apache/opendal/pull/4762

### Chore
* chore: use more portable binutils  by @tisonkun in https://github.com/apache/opendal/pull/4713
* chore(deps): bump clap from 4.5.6 to 4.5.7 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/4728
* chore(deps): bump url from 2.5.0 to 2.5.1 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/4729
* chore(binding/python): Upgrade pyo3 to 0.21 by @reswqa in https://github.com/apache/opendal/pull/4734
* chore: Make 1.79 clippy happy by @Xuanwo in https://github.com/apache/opendal/pull/4731
* chore(docs): Add new line in lone services by @Xuanwo in https://github.com/apache/opendal/pull/4743
* chore: Bump versions to prepare v0.47.1 release by @Xuanwo in https://github.com/apache/opendal/pull/4759

## [v0.47.0] - 2024-06-07

### Added
* feat(core/types): change oio::BlockingReader to `Arc<dyn oio::BlockingReader>` by @hoslo in https://github.com/apache/opendal/pull/4577
* fix: format_object_meta should not require metakeys that don't exist by @rebasedming in https://github.com/apache/opendal/pull/4582
* feat: add checksums to MultiPartComplete by @JWackerbauer in https://github.com/apache/opendal/pull/4580
* feat(doc): update object_store_opendal README by @hanxuanliang in https://github.com/apache/opendal/pull/4606
* feat(services/aliyun-drive): support AliyunDrive by @yuchanns in https://github.com/apache/opendal/pull/4585
* feat(bindings/python): Update type annotations by @3ok in https://github.com/apache/opendal/pull/4630
* feat: implement OperatorInputStream and OperatorOutputStream by @tisonkun in https://github.com/apache/opendal/pull/4626
* feat(bench): add buffer benchmark by @zjregee in https://github.com/apache/opendal/pull/4603
* feat: Add Executor struct and Execute trait by @Xuanwo in https://github.com/apache/opendal/pull/4648
* feat: Add executor in OpXxx and Operator by @Xuanwo in https://github.com/apache/opendal/pull/4649
* feat: Implement and refactor concurrent tasks for multipart write by @Xuanwo in https://github.com/apache/opendal/pull/4653
* feat(core/types): blocking remove_all for object storage based services by @TennyZhuang in https://github.com/apache/opendal/pull/4665
* feat(core): Streaming reading while chunk is not set by @Xuanwo in https://github.com/apache/opendal/pull/4658
* feat(core): Add more context in error context by @Xuanwo in https://github.com/apache/opendal/pull/4673
* feat: init ovfs by @zjregee in https://github.com/apache/opendal/pull/4652
* feat: Implement retry for streaming based read by @Xuanwo in https://github.com/apache/opendal/pull/4683
* feat(core): Implement TimeoutLayer for concurrent tasks by @Xuanwo in https://github.com/apache/opendal/pull/4688
* feat(core): Add reader size check in complete reader by @Xuanwo in https://github.com/apache/opendal/pull/4690
* feat(core): Azblob supports azure workload identity by @Xuanwo in https://github.com/apache/opendal/pull/4705
### Changed
* refactor(core): Align naming for `AccessorDyn` by @morristai in https://github.com/apache/opendal/pull/4574
* refactor(core): core doesn't expose invalid input error anymore by @Xuanwo in https://github.com/apache/opendal/pull/4632
* refactor(core): Return unexpected error while content incomplete happen by @Xuanwo in https://github.com/apache/opendal/pull/4633
* refactor(core): Change Read's behavior to ensure it reads the exact size of data by @Xuanwo in https://github.com/apache/opendal/pull/4634
* refactor(bin/ofs): Fuse API by @ho-229 in https://github.com/apache/opendal/pull/4637
* refactor(binding/java)!: rename blocking and async operator by @tisonkun in https://github.com/apache/opendal/pull/4641
* refactor(core): Use concurrent tasks to refactor block write by @Xuanwo in https://github.com/apache/opendal/pull/4692
* refactor(core): Migrate RangeWrite to ConcurrentTasks by @Xuanwo in https://github.com/apache/opendal/pull/4696
### Fixed
* fix(devcontainer/post_create.sh): change pnpm@stable to pnpm@latest by @GG2002 in https://github.com/apache/opendal/pull/4584
* fix(bin/ofs): privileged mount crashes when external umount by @ho-229 in https://github.com/apache/opendal/pull/4586
* fix(bin/ofs): ofs read only mount by @ho-229 in https://github.com/apache/opendal/pull/4602
* fix(raw): Allow retrying request while decoding response failed by @Xuanwo in https://github.com/apache/opendal/pull/4612
* fix(core): return None if metadata unavailable by @NKID00 in https://github.com/apache/opendal/pull/4613
* fix(bindings/python): Use abi3 and increase MSPV to 3.11 by @Xuanwo in https://github.com/apache/opendal/pull/4623
* fix: Fetch the content length while end_bound is unknown by @Xuanwo in https://github.com/apache/opendal/pull/4631
* fix: ofs write behavior by @ho-229 in https://github.com/apache/opendal/pull/4617
* fix(core/types): remove_all not work under object-store backend by @TennyZhuang in https://github.com/apache/opendal/pull/4659
* fix(ofs): Close file during flush by @Xuanwo in https://github.com/apache/opendal/pull/4680
* fix(core): RetryLayer could panic when other threads raises panic by @Xuanwo in https://github.com/apache/opendal/pull/4685
* fix(core/prometheus): Fix metrics from prometheus not correct for reader by @Xuanwo in https://github.com/apache/opendal/pull/4691
* fix(core/oio): Make ConcurrentTasks cancel safe by only pop after ready by @Xuanwo in https://github.com/apache/opendal/pull/4707
### Docs
* docs: fix Operator::writer doc comment by @mnpw in https://github.com/apache/opendal/pull/4605
* doc: explain GCS authentication options by @jokester in https://github.com/apache/opendal/pull/4671
* docs: Fix all broken links by @Xuanwo in https://github.com/apache/opendal/pull/4694
* docs: Add upgrade note for v0.47 by @Xuanwo in https://github.com/apache/opendal/pull/4698
* docs: Add panics declare for TimeoutLayer and RetryLayer by @Xuanwo in https://github.com/apache/opendal/pull/4702
### CI
* ci: upgrade typos to 1.21.0 and ignore changelog by @hezhizhen in https://github.com/apache/opendal/pull/4601
* ci: Disable jfrog webdav tests for it's keeping failed by @Xuanwo in https://github.com/apache/opendal/pull/4607
* ci: use official typos github action by @shoothzj in https://github.com/apache/opendal/pull/4635
* build(deps): upgrade crc32c to 0.6.6 for nightly toolchain by @tisonkun in https://github.com/apache/opendal/pull/4650
### Chore
* chore: fixup release docs and scripts by @tisonkun in https://github.com/apache/opendal/pull/4571
* chore: Make rust 1.78 happy by @Xuanwo in https://github.com/apache/opendal/pull/4572
* chore: fixup items identified in releases by @tisonkun in https://github.com/apache/opendal/pull/4578
* chore(deps): bump peaceiris/actions-gh-pages from 3.9.2 to 4.0.0 by @dependabot in https://github.com/apache/opendal/pull/4561
* chore: Contribute ParadeDB by @philippemnoel in https://github.com/apache/opendal/pull/4587
* chore(deps): bump rusqlite from 0.29.0 to 0.31.0 in /core by @dependabot in https://github.com/apache/opendal/pull/4556
* chore(deps): Bump object_store to 0.10 by @TCeason in https://github.com/apache/opendal/pull/4590
* chore: remove outdated scan op in all docs.md by @GG2002 in https://github.com/apache/opendal/pull/4600
* chore: tidy services in project file by @suyanhanx in https://github.com/apache/opendal/pull/4621
* chore(deps): make crc32c optional under services-s3 by @xxchan in https://github.com/apache/opendal/pull/4643
* chore(core): Fix unit tests by @Xuanwo in https://github.com/apache/opendal/pull/4684
* chore(core): Add unit and bench tests for concurrent tasks by @Xuanwo in https://github.com/apache/opendal/pull/4695
* chore: bump version to 0.47.0 by @tisonkun in https://github.com/apache/opendal/pull/4701
* chore: Update changelogs for v0.47 by @Xuanwo in https://github.com/apache/opendal/pull/4706

## [v0.46.0] - 2024-05-02

### Added
* feat(services/github): add github contents support by @hoslo in https://github.com/apache/opendal/pull/4281
* feat: Allow selecting webpki for reqwest by @arlyon in https://github.com/apache/opendal/pull/4303
* feat(services/swift): add support for storage_url configuration in swift service by @zjregee in https://github.com/apache/opendal/pull/4302
* feat(services/swift): add ceph test setup for swift by @zjregee in https://github.com/apache/opendal/pull/4307
* docs(website): add local content search based on lunr plugin by @m1911star in https://github.com/apache/opendal/pull/4348
* feat(services/sled): add SledConfig by @yufan022 in https://github.com/apache/opendal/pull/4351
* feat : Implementing config for part of services by @AnuRage-git in https://github.com/apache/opendal/pull/4344
* feat(bindings/java): explicit async runtime  by @tisonkun in https://github.com/apache/opendal/pull/4376
* feat(services/surrealdb): support surrealdb service by @yufan022 in https://github.com/apache/opendal/pull/4375
* feat(bindings/java): avoid double dispose NativeObject by @tisonkun in https://github.com/apache/opendal/pull/4377
* feat : Implement config for services/foundationdb by @AnuRage-git in https://github.com/apache/opendal/pull/4355
* feat: add ofs ctrl-c exit unmount hook by @oowl in https://github.com/apache/opendal/pull/4393
* feat: Implement RFC-4382 Range Based Read by @Xuanwo in https://github.com/apache/opendal/pull/4381
* feat(ofs): rename2 lseek copy_file_range getattr API support by @oowl in https://github.com/apache/opendal/pull/4395
* feat(services/github): make access_token optional by @hoslo in https://github.com/apache/opendal/pull/4404
* feat(core/oio): Add readable buf by @Xuanwo in https://github.com/apache/opendal/pull/4407
* feat(ofs): add freebsd OS support by @oowl in https://github.com/apache/opendal/pull/4403
* feat(core/raw/oio): Add Writable Buf by @Xuanwo in https://github.com/apache/opendal/pull/4410
* feat(bin/ofs): Add behavior test for ofs by @ho-229 in https://github.com/apache/opendal/pull/4373
* feat(core/raw/buf): Reduce one allocation by `Arc::from_iter` by @Xuanwo in https://github.com/apache/opendal/pull/4440
* feat: ?Send async trait for HttpBackend when the target is wasm32 by @waynexia in https://github.com/apache/opendal/pull/4444
* feat: add `HttpClient::with()` constructor by @waynexia in https://github.com/apache/opendal/pull/4447
* feat: Move Buffer as public API by @Xuanwo in https://github.com/apache/opendal/pull/4450
* feat: Optimize buffer implementation and add stream support by @Xuanwo in https://github.com/apache/opendal/pull/4458
* feat(core): Implement FromIterator for Buffer by @Xuanwo in https://github.com/apache/opendal/pull/4459
* feat(services/ftp): Support multiple write by @xxxuuu in https://github.com/apache/opendal/pull/4425
* feat(raw/oio): block write change to buffer by @hoslo in https://github.com/apache/opendal/pull/4466
* feat(core): Implement read and read_into for Reader by @Xuanwo in https://github.com/apache/opendal/pull/4467
* feat(core): Implement into_stream for Reader by @Xuanwo in https://github.com/apache/opendal/pull/4473
* feat(core): Tune buffer operations  based on benchmark results by @Xuanwo in https://github.com/apache/opendal/pull/4468
* feat(raw/oio): Use `Buffer` as cache in `RangeWrite` by @reswqa in https://github.com/apache/opendal/pull/4476
* feat(raw/oio): Use `Buffer` as cache in `OneshotWrite` by @reswqa in https://github.com/apache/opendal/pull/4477
* feat: add list/copy/rename for dropbox by @zjregee in https://github.com/apache/opendal/pull/4424
* feat(core): Implement write and write_from for Writer by @zjregee in https://github.com/apache/opendal/pull/4482
* feat(core): Add auto ranged read and concurrent read support by @Xuanwo in https://github.com/apache/opendal/pull/4486
* feat(core): Implement fetch for Reader by @Xuanwo in https://github.com/apache/opendal/pull/4488
* feat(core): Allow concurrent reading on bytes stream by @Xuanwo in https://github.com/apache/opendal/pull/4499
* feat: provide send-wrapper to contidionally implement Send for operators by @waynexia in https://github.com/apache/opendal/pull/4443
* feat(bin/ofs): privileged mount by @ho-229 in https://github.com/apache/opendal/pull/4507
* feat(services/compfs): init compfs by @George-Miao in https://github.com/apache/opendal/pull/4519
* feat(raw/oio): Add PooledBuf to allow reuse buffer by @Xuanwo in https://github.com/apache/opendal/pull/4522
* feat(services/fs): Add PooledBuf in fs to allow reusing memory by @Xuanwo in https://github.com/apache/opendal/pull/4525
* feat(core): add access methods for `Buffer` by @George-Miao in https://github.com/apache/opendal/pull/4530
* feat(core): implement `IoBuf` for `Buffer` by @George-Miao in https://github.com/apache/opendal/pull/4532
* feat(services/compfs): compio runtime and compfs structure by @George-Miao in https://github.com/apache/opendal/pull/4534
* feat(core): change Result to default error by @George-Miao in https://github.com/apache/opendal/pull/4535
* feat(services/github): support list recursive by @hoslo in https://github.com/apache/opendal/pull/4423
* feat: Add crc32c checksums to S3 Service by @JWackerbauer in https://github.com/apache/opendal/pull/4533
* feat: Add into_bytes_sink for Writer by @Xuanwo in https://github.com/apache/opendal/pull/4541
### Changed
* refactor(core/raw): Migrate `oio::Read` to async in trait by @Xuanwo in https://github.com/apache/opendal/pull/4336
* refactor(core/raw): Align oio::BlockingRead API with oio::Read by @Xuanwo in https://github.com/apache/opendal/pull/4349
* refactor(core/oio): Migrate `oio::List` to async fn in trait by @Xuanwo in https://github.com/apache/opendal/pull/4352
* refactor(core/raw): Migrate oio::Write from WriteBuf to Bytes by @Xuanwo in https://github.com/apache/opendal/pull/4356
* refactor(core/raw): Migrate `oio::Write` to async in trait by @Xuanwo in https://github.com/apache/opendal/pull/4358
* refactor(bindings/python): Change the return type of `File::read` to `PyResult<&PyBytes>` by @reswqa in https://github.com/apache/opendal/pull/4360
* refactor(core/raw): Cleanup not used `oio::WriteBuf` and `oio::ChunkedBytes` after refactor by @Xuanwo in https://github.com/apache/opendal/pull/4361
* refactor: Remove reqwest related features by @Xuanwo in https://github.com/apache/opendal/pull/4365
* refactor(bin/ofs): make `ofs` API public by @ho-229 in https://github.com/apache/opendal/pull/4387
* refactor: Impl into_futures_io_async_write for Writer by @Xuanwo in https://github.com/apache/opendal/pull/4406
* refactor(core/oio): Use ReadableBuf to remove extra clone during write by @Xuanwo in https://github.com/apache/opendal/pull/4408
* refactor(core/raw/oio): Mark oio::Write::write as an unsafe function by @Xuanwo in https://github.com/apache/opendal/pull/4413
* refactor(core/raw/oio): Use oio buffer in write by @Xuanwo in https://github.com/apache/opendal/pull/4436
* refactor(core/raw): oio::Write is safe operation now by @Xuanwo in https://github.com/apache/opendal/pull/4438
* refactor(core): Use Buffer in http request by @Xuanwo in https://github.com/apache/opendal/pull/4460
* refactor(core): Polish FuturesBytesStream by avoiding extra copy by @Xuanwo in https://github.com/apache/opendal/pull/4474
* refactor: Use Buffer as cache in MultipartWrite by @tisonkun in https://github.com/apache/opendal/pull/4493
* refactor: kv::adapter should use Buffer (read part) by @tisonkun in https://github.com/apache/opendal/pull/4494
* refactor: typed_kv::adapter should use Buffer by @tisonkun in https://github.com/apache/opendal/pull/4497
* refactor: kv::adapter should use Buffer (write part) by @tisonkun in https://github.com/apache/opendal/pull/4496
* refactor: Migrate FuturesAsyncReader to stream based by @Xuanwo in https://github.com/apache/opendal/pull/4508
* refactor(services/fs): Extract FsCore from FsBackend by @Xuanwo in https://github.com/apache/opendal/pull/4523
* refactor(core): Migrate `Accessor` to async fn in trait by @George-Miao in https://github.com/apache/opendal/pull/4562
* refactor(core): Align naming for `Accessor` by @George-Miao in https://github.com/apache/opendal/pull/4564
### Fixed
* fix(bin/ofs): crashes when fh=None by @ho-229 in https://github.com/apache/opendal/pull/4297
* fix(services/hdfs): fix poll_close when retry by @hoslo in https://github.com/apache/opendal/pull/4309
* fix: fix main CI  by @xxchan in https://github.com/apache/opendal/pull/4319
* fix: fix ghac CI by @xxchan in https://github.com/apache/opendal/pull/4320
* fix(services/dropbox): fix dropbox batch test panic in ci by @zjregee in https://github.com/apache/opendal/pull/4329
* fix(services/hdfs-native): remove unsupported capabilities by @jihuayu in https://github.com/apache/opendal/pull/4333
* fix(bin/ofs): build failed when target_os != linux by @ho-229 in https://github.com/apache/opendal/pull/4334
* fix(bin/ofs): only memory backend available by @ho-229 in https://github.com/apache/opendal/pull/4353
* fix(bindings/python): Fix the semantic of `size` argument for python `File::read` by @reswqa in https://github.com/apache/opendal/pull/4359
* fix(bindings/python): `File::write` should return the written bytes by @reswqa in https://github.com/apache/opendal/pull/4367
* fix(services/s3): omit default ports by @yufan022 in https://github.com/apache/opendal/pull/4379
* fix(core/services/gdrive): Fix gdrive test failed for refresh token by @Xuanwo in https://github.com/apache/opendal/pull/4435
* fix(core/services/cos): Don't allow empty credential by @Xuanwo in https://github.com/apache/opendal/pull/4457
* fix(oay): support `WebdavFile` continuous reading and writing by @G-XD in https://github.com/apache/opendal/pull/4374
* fix(integrations/webdav): Fix read file API changes by @Xuanwo in https://github.com/apache/opendal/pull/4462
* fix(s3): don't delete bucket by @sameer in https://github.com/apache/opendal/pull/4430
* fix(core): Buffer offset misuse by @George-Miao in https://github.com/apache/opendal/pull/4481
* fix(core): Read chunk should respect users input by @Xuanwo in https://github.com/apache/opendal/pull/4487
* fix(services/cos): fix mdx by @hoslo in https://github.com/apache/opendal/pull/4510
* fix: minor doc issue by @George-Miao in https://github.com/apache/opendal/pull/4517
* fix(website): community sync calendar iframe by @suyanhanx in https://github.com/apache/opendal/pull/4549
* fix: community sync calendar iframe load failed by @suyanhanx in https://github.com/apache/opendal/pull/4550
### Docs
* docs: Add gsoc proposal guide by @Xuanwo in https://github.com/apache/opendal/pull/4287
* docs: Add blog for apache opendal participates in gsoc 2024 by @Xuanwo in https://github.com/apache/opendal/pull/4288
* docs: Fix and improve docs for presign operations by @Xuanwo in https://github.com/apache/opendal/pull/4294
* docs: Polish the docs for writer by @Xuanwo in https://github.com/apache/opendal/pull/4296
* docs: Add redirect for discord and maillist by @Xuanwo in https://github.com/apache/opendal/pull/4312
* docs: update swift docs by @zjregee in https://github.com/apache/opendal/pull/4327
* docs: publish docs for object-store-opendal by @zjregee in https://github.com/apache/opendal/pull/4328
* docs: fix redirect error in object-store-opendal by @zjregee in https://github.com/apache/opendal/pull/4332
* docs(website): fix grammar and spelling by @jbampton in https://github.com/apache/opendal/pull/4340
* docs: fix nodejs binding docs footer copyright by @m1911star in https://github.com/apache/opendal/pull/4346
* docs(bin/ofs): update README by @ho-229 in https://github.com/apache/opendal/pull/4354
* docs(services/gcs): update gcs credentials description by @zjregee in https://github.com/apache/opendal/pull/4362
* docs(bindings/python): ipynb examples for polars and pandas by @reswqa in https://github.com/apache/opendal/pull/4368
* docs(core): correct the doc for icloud and memcached by @Kilerd in https://github.com/apache/opendal/pull/4422
* docs: polish release doc for vote result by @suyanhanx in https://github.com/apache/opendal/pull/4429
* docs: Update links to o.a.o/discord by @Xuanwo in https://github.com/apache/opendal/pull/4442
* docs: add layers example by @zjregee in https://github.com/apache/opendal/pull/4449
* docs: publish docs for dav-server-opendalfs by @zjregee in https://github.com/apache/opendal/pull/4503
* docs: Add docs for read_with and reader_with by @Xuanwo in https://github.com/apache/opendal/pull/4516
* docs: Add upgrade doc for rust core 0.46 by @Xuanwo in https://github.com/apache/opendal/pull/4543
* docs: update the outdated download link by @suyanhanx in https://github.com/apache/opendal/pull/4546
### CI
* ci(binding/java): Don't create too many files in CI by @Xuanwo in https://github.com/apache/opendal/pull/4289
* ci: Address Node.js 16 actions are deprecated by @Xuanwo in https://github.com/apache/opendal/pull/4293
* ci: Disable vsftpd test for it's keeping failure by @Xuanwo in https://github.com/apache/opendal/pull/4295
* build: remove `service-*` from default features by @xxchan in https://github.com/apache/opendal/pull/4311
* ci: upgrade typos in ci by @Young-Flash in https://github.com/apache/opendal/pull/4322
* ci: Disable R2 until we figure what happened by @Xuanwo in https://github.com/apache/opendal/pull/4323
* ci(s3/minio): Disable IMDSv2 for mini anonymous tests by @Xuanwo in https://github.com/apache/opendal/pull/4326
* ci: Fix unit tests missing protoc by @Xuanwo in https://github.com/apache/opendal/pull/4369
* ci: Fix foundationdb not setup for unit test by @Xuanwo in https://github.com/apache/opendal/pull/4370
* ci: bump license header formatter by @tisonkun in https://github.com/apache/opendal/pull/4390
* ci: fix dragonflydb docker-compose healthcheck broken by @oowl in https://github.com/apache/opendal/pull/4431
* ci: fix planner for bin ofs by @ho-229 in https://github.com/apache/opendal/pull/4448
* ci: Revert oay changes to fix CI by @Xuanwo in https://github.com/apache/opendal/pull/4463
* ci: Fix CI for all bindings by @Xuanwo in https://github.com/apache/opendal/pull/4469
* ci: Disable dropbox test until #4484 fixed by @Xuanwo in https://github.com/apache/opendal/pull/4485
* ci: Fix build of python binding for chunk write changes by @Xuanwo in https://github.com/apache/opendal/pull/4529
* build(core): bump compio version to v0.10.0 by @George-Miao in https://github.com/apache/opendal/pull/4531
* ci: Disable tikv for pingcap's mirror is unstable by @Xuanwo in https://github.com/apache/opendal/pull/4538
* build: staging website  by @tisonkun in https://github.com/apache/opendal/pull/4565
* build: fixup follow asfyaml rules by @tisonkun in https://github.com/apache/opendal/pull/4566
### Chore
* chore(deps): bump tempfile from 3.9.0 to 3.10.1 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/4298
* chore(deps): bump wasm-bindgen-test from 0.3.40 to 0.3.41 in /core by @dependabot in https://github.com/apache/opendal/pull/4299
* chore(deps): bump log from 0.4.20 to 0.4.21 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/4301
* chore(services/redis): Fix docs & comments typos by @AJIOB in https://github.com/apache/opendal/pull/4306
* chore(editorconfig): add `yaml` file type by @jbampton in https://github.com/apache/opendal/pull/4339
* chore(editorconfig): add `rdf` file type as `indent_size = 2` by @jbampton in https://github.com/apache/opendal/pull/4341
* chore(editorconfig): order entries and add `indent_style = tab` for Go by @jbampton in https://github.com/apache/opendal/pull/4342
* chore(labeler): order the GitHub labeler labels by @jbampton in https://github.com/apache/opendal/pull/4343
* chore: Cleanup of oio::Read, docs, comments, naming by @Xuanwo in https://github.com/apache/opendal/pull/4345
* chore: Remove not exist read operations by @Xuanwo in https://github.com/apache/opendal/pull/4412
* chore(deps): bump toml from 0.8.10 to 0.8.12 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/4418
* chore(deps): bump toml from 0.8.10 to 0.8.12 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/4420
* chore(deps): bump tokio from 1.36.0 to 1.37.0 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/4419
* chore(deps): bump prometheus-client from 0.22.1 to 0.22.2 in /core by @dependabot in https://github.com/apache/opendal/pull/4417
* chore: update copyright year to 2024 in NOTICE by @shoothzj in https://github.com/apache/opendal/pull/4433
* chore: Bump bytes to 1.6 to avoid compileing error by @Xuanwo in https://github.com/apache/opendal/pull/4472
* chore: Add output types in OperatorFutures by @Xuanwo in https://github.com/apache/opendal/pull/4475
* chore(core): Use reader's chunk size instead by @Xuanwo in https://github.com/apache/opendal/pull/4489
* chore: sort tomls by taplo by @tisonkun in https://github.com/apache/opendal/pull/4491
* chore(core): Align Reader and Writer's API design by @Xuanwo in https://github.com/apache/opendal/pull/4498
* chore: Add docs and tests for reader related types by @Xuanwo in https://github.com/apache/opendal/pull/4513
* chore: Reorganize the blocking reader layout by @Xuanwo in https://github.com/apache/opendal/pull/4514
* chore: Align with chunk instead of confusing buffer by @Xuanwo in https://github.com/apache/opendal/pull/4528
* chore: Refactor Write and BlockingWrite API by @Xuanwo in https://github.com/apache/opendal/pull/4540
* chore(core): Change std result to opendal result in core by @tannal in https://github.com/apache/opendal/pull/4542
* chore: fixup download links by @tisonkun in https://github.com/apache/opendal/pull/4547
* chore(deps): bump clap from 4.5.1 to 4.5.4 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/4557
* chore(deps): bump anyhow from 1.0.80 to 1.0.82 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/4559
* chore(deps): bump libc from 0.2.153 to 0.2.154 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/4558

## [v0.45.1] - 2024-02-22

### Added
* feat(services/vercel_blob): support vercel blob by @hoslo in https://github.com/apache/opendal/pull/4103
* feat(bindings/python): add ruff as linter by @asukaminato0721 in https://github.com/apache/opendal/pull/4135
* feat(services/hdfs-native): Add capabilities for hdfs-native service by @jihuayu in https://github.com/apache/opendal/pull/4174
* feat(services/sqlite): Add list capability supported for sqlite by @jihuayu in https://github.com/apache/opendal/pull/4180
* feat(services/azblob): support multi write for azblob by @wcy-fdu in https://github.com/apache/opendal/pull/4181
* feat(release): Implement releasing OpenDAL components separately by @Xuanwo in https://github.com/apache/opendal/pull/4196
* feat: object store adapter based on v0.9 by @waynexia in https://github.com/apache/opendal/pull/4233
* feat(bin/ofs): implement fuse for linux by @ho-229 in https://github.com/apache/opendal/pull/4179
* feat(services/memcached): change to binary protocol by @hoslo in https://github.com/apache/opendal/pull/4252
* feat(services/memcached): setup auth test for memcached by @hoslo in https://github.com/apache/opendal/pull/4259
* feat(services/yandex_disk): setup test for yandex disk by @hoslo in https://github.com/apache/opendal/pull/4264
* feat: add ci support for ceph_rados by @ZhengLin-Li in https://github.com/apache/opendal/pull/4191
* feat: Implement Config for part of services by @Xuanwo in https://github.com/apache/opendal/pull/4277
* feat: add jfrog test setup for webdav by @zjregee in https://github.com/apache/opendal/pull/4265
### Changed
* refactor(bindings/python): simplify async writer aexit by @suyanhanx in https://github.com/apache/opendal/pull/4128
* refactor(service/d1): Add D1Config by @jihuayu in https://github.com/apache/opendal/pull/4129
* refactor: Rewrite webdav to improve code quality by @Xuanwo in https://github.com/apache/opendal/pull/4280
### Fixed
* fix: Azdls returns 403 while continuation contains `=` by @Xuanwo in https://github.com/apache/opendal/pull/4105
* fix(bindings/python): missed to call close for the file internally by @zzl221000 in https://github.com/apache/opendal/pull/4122
* fix(bindings/python): sync writer exit close raise error by @suyanhanx in https://github.com/apache/opendal/pull/4127
* fix(services/chainsafe): fix 423 http status by @hoslo in https://github.com/apache/opendal/pull/4148
* fix(services/webdav): Add possibility to answer without response if file isn't exist by @AJIOB in https://github.com/apache/opendal/pull/4170
* fix(services/webdav): Recreate root directory if need by @AJIOB in https://github.com/apache/opendal/pull/4173
* fix(services/webdav): remove base_dir component by @hoslo in https://github.com/apache/opendal/pull/4231
* fix(core): Poll TimeoutLayer::sleep once to make sure timer registered by @Xuanwo in https://github.com/apache/opendal/pull/4230
* fix(services/webdav): Fix endpoint suffix not handled by @Xuanwo in https://github.com/apache/opendal/pull/4257
* fix(services/webdav): Fix possible error with value loosing from config by @AJIOB in https://github.com/apache/opendal/pull/4262
### Docs
* docs: add request for add secrets of services by @suyanhanx in https://github.com/apache/opendal/pull/4104
* docs(website): announce release v0.45.0 to news by @morristai in https://github.com/apache/opendal/pull/4152
* docs(services/gdrive): Update Google Drive capabilities list docs by @jihuayu in https://github.com/apache/opendal/pull/4158
* docs: Fix docs build by @Xuanwo in https://github.com/apache/opendal/pull/4162
* docs: add docs for Ceph Rados Gateway S3 by @ZhengLin-Li in https://github.com/apache/opendal/pull/4190
* docs: Fix typo in `core/src/services/http/docs.md` by @jbampton in https://github.com/apache/opendal/pull/4226
* docs: Fix spelling in Rust files by @jbampton in https://github.com/apache/opendal/pull/4227
* docs: fix typo in `website/README.md` by @jbampton in https://github.com/apache/opendal/pull/4228
* docs: fix spelling by @jbampton in https://github.com/apache/opendal/pull/4229
* docs: fix spelling; change `github` to `GitHub` by @jbampton in https://github.com/apache/opendal/pull/4232
* docs: fix typo by @jbampton in https://github.com/apache/opendal/pull/4234
* docs: fix typo in `bindings/c/CONTRIBUTING.md` by @jbampton in https://github.com/apache/opendal/pull/4235
* docs: fix spelling in code comments by @jbampton in https://github.com/apache/opendal/pull/4236
* docs: fix spelling in `CONTRIBUTING` by @jbampton in https://github.com/apache/opendal/pull/4237
* docs: fix Markdown link in `bindings/README.md` by @jbampton in https://github.com/apache/opendal/pull/4238
* docs: fix links and spelling in Markdown by @jbampton in https://github.com/apache/opendal/pull/4239
* docs: fix grammar and spelling in Markdown in `examples/rust` by @jbampton in https://github.com/apache/opendal/pull/4241
* docs: remove unneeded duplicate words from Rust files by @jbampton in https://github.com/apache/opendal/pull/4243
* docs: fix grammar and spelling in Markdown by @jbampton in https://github.com/apache/opendal/pull/4245
* docs: Add architectural.png to website by @Xuanwo in https://github.com/apache/opendal/pull/4261
* docs: Re-org project README by @Xuanwo in https://github.com/apache/opendal/pull/4260
* docs: order the README `Who is using OpenDAL` list  by @jbampton in https://github.com/apache/opendal/pull/4263
### CI
* ci: Use old version of seafile mc instead by @Xuanwo in https://github.com/apache/opendal/pull/4107
* ci: Refactor workflows layout by @Xuanwo in https://github.com/apache/opendal/pull/4139
* ci: Migrate hdfs default setup by @Xuanwo in https://github.com/apache/opendal/pull/4140
* ci: Refactor check.sh into check.py to get ready for multi components release by @Xuanwo in https://github.com/apache/opendal/pull/4159
* ci: Add test case for hdfs over gcs bucket by @ArmandoZ in https://github.com/apache/opendal/pull/4145
* ci: Add hdfs test case for s3 by @ArmandoZ in https://github.com/apache/opendal/pull/4184
* ci: Add hdfs test case for azurite by @ArmandoZ in https://github.com/apache/opendal/pull/4185
* ci: Add support for releasing all rust packages by @Xuanwo in https://github.com/apache/opendal/pull/4200
* ci: Fix dependabot not update by @Xuanwo in https://github.com/apache/opendal/pull/4202
* ci: reduce the open pull request limits to 1 by @jbampton in https://github.com/apache/opendal/pull/4225
* ci: Remove version suffix from package versions by @Xuanwo in https://github.com/apache/opendal/pull/4254
* ci: Fix fuzz test for minio s3 name change by @Xuanwo in https://github.com/apache/opendal/pull/4266
* ci: Mark python 3.13 is not supported by @Xuanwo in https://github.com/apache/opendal/pull/4269
* ci: Disable yandex disk test for too slow by @Xuanwo in https://github.com/apache/opendal/pull/4274
* ci: Split python CI into release and checks by @Xuanwo in https://github.com/apache/opendal/pull/4275
* ci(release): Make sure LICENSE and NOTICE files are included by @Xuanwo in https://github.com/apache/opendal/pull/4283
* ci(release): Refactor and merge the check.py into verify.py by @Xuanwo in https://github.com/apache/opendal/pull/4284
### Chore
* chore(deps): bump actions/cache from 3 to 4 by @dependabot in https://github.com/apache/opendal/pull/4118
* chore(deps): bump toml from 0.8.8 to 0.8.9 by @dependabot in https://github.com/apache/opendal/pull/4109
* chore(deps): bump dav-server from 0.5.7 to 0.5.8 by @dependabot in https://github.com/apache/opendal/pull/4111
* chore(deps): bump assert_cmd from 2.0.12 to 2.0.13 by @dependabot in https://github.com/apache/opendal/pull/4112
* chore(deps): bump actions/setup-dotnet from 3 to 4 by @dependabot in https://github.com/apache/opendal/pull/4115
* chore(deps): bump mongodb from 2.7.1 to 2.8.0 by @dependabot in https://github.com/apache/opendal/pull/4110
* chore(deps): bump quick-xml from 0.30.0 to 0.31.0 by @dependabot in https://github.com/apache/opendal/pull/4113
* chore: Make every components separate, remove workspace by @Xuanwo in https://github.com/apache/opendal/pull/4134
* chore: Fix build of core by @Xuanwo in https://github.com/apache/opendal/pull/4137
* chore: Fix workflow links for opendal by @Xuanwo in https://github.com/apache/opendal/pull/4147
* chore(website): Bump download link for 0.45.0 release by @morristai in https://github.com/apache/opendal/pull/4149
* chore: Fix name DtraceLayerWrapper by @jayvdb in https://github.com/apache/opendal/pull/4165
* chore: Align core version by @Xuanwo in https://github.com/apache/opendal/pull/4197
* chore: update benchmark doc by @wcy-fdu in https://github.com/apache/opendal/pull/4201
* chore(deps): bump clap from 4.4.18 to 4.5.1 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/4221
* chore(deps): bump serde from 1.0.196 to 1.0.197 in /bin/oay by @dependabot in https://github.com/apache/opendal/pull/4214
* chore(deps): bump anyhow from 1.0.79 to 1.0.80 in /bin/ofs by @dependabot in https://github.com/apache/opendal/pull/4209
* chore(deps): bump anyhow from 1.0.79 to 1.0.80 in /bin/oli by @dependabot in https://github.com/apache/opendal/pull/4216
* chore(deps): bump cacache from 12.0.0 to 13.0.0 in /core by @dependabot in https://github.com/apache/opendal/pull/4215

## [v0.45.0] - 2024-01-29

### Added
* feat(ofs): introduce ofs execute bin by @oowl in https://github.com/apache/opendal/pull/4033
* feat: add a missing news by @WenyXu in https://github.com/apache/opendal/pull/4056
* feat(services/koofr): set test for koofr by @suyanhanx in https://github.com/apache/opendal/pull/4050
* feat(layers/dtrace): Support User Statically-Defined Tracing(aka USDT) on Linux by @Zheaoli in https://github.com/apache/opendal/pull/4053
* feat(website): add missing news and organize the onboarding guide by @morristai in https://github.com/apache/opendal/pull/4072
### Changed
* refactor!: avoid hard dep to tokio rt by @tisonkun in https://github.com/apache/opendal/pull/4061
### Fixed
* fix(examples/cpp): display the results to standard output. by @SYaoJun in https://github.com/apache/opendal/pull/4040
* fix(service/icloud):Missing 'X-APPLE-WEBAUTH-USER cookie' and URL initialized failed by @bokket in https://github.com/apache/opendal/pull/4029
* fix: Implement timeout layer correctly by using timeout by @Xuanwo in https://github.com/apache/opendal/pull/4059
* fix(koofr): create_dir when exist by @hoslo in https://github.com/apache/opendal/pull/4062
* fix(seafile): test_list_dir_with_metakey by @hoslo in https://github.com/apache/opendal/pull/4063
* fix: list path recursive should not return path itself by @youngsofun in https://github.com/apache/opendal/pull/4067
### Docs
* docs: Remove not needed actions in release guide by @Xuanwo in https://github.com/apache/opendal/pull/4037
* docs: fix spelling errors in README.md by @SYaoJun in https://github.com/apache/opendal/pull/4039
* docs: New PMC member Liuqing Yue by @Xuanwo in https://github.com/apache/opendal/pull/4047
* docs: New Committer Yang Shuai by @Xuanwo in https://github.com/apache/opendal/pull/4054
* docs(services/sftp): add more explanation for endpoint config by @silver-ymz in https://github.com/apache/opendal/pull/4055
### CI
* ci(services/s3): Use minio/minio image instead by @Xuanwo in https://github.com/apache/opendal/pull/4070
* ci: Fix CI after moving out of workspacs by @Xuanwo in https://github.com/apache/opendal/pull/4081
### Chore
* chore: Delete bindings/ruby/cucumber.yml by @tisonkun in https://github.com/apache/opendal/pull/4030
* chore(website): Bump download link for 0.44.2 release by @Zheaoli in https://github.com/apache/opendal/pull/4034
* chore(website): Update the release tips by @Zheaoli in https://github.com/apache/opendal/pull/4036
* chore: add doap file by @tisonkun in https://github.com/apache/opendal/pull/4038
* chore(website): Add extra artifacts check process in release document by @Zheaoli in https://github.com/apache/opendal/pull/4041
* chore(bindings/dotnet): update cargo.lock and set up ci by @suyanhanx in https://github.com/apache/opendal/pull/4084
* chore(bindings/dotnet): build os detect by @suyanhanx in https://github.com/apache/opendal/pull/4085
* chore(bindings/ocaml): pinning OCaml binding opendal version for release by @Ranxy in https://github.com/apache/opendal/pull/4086


## [v0.44.2] - 2023-01-19

### Added
* feat: add behavior tests for blocking buffer reader by @WenyXu in https://github.com/apache/opendal/pull/3872
* feat(services): add pcloud support by @hoslo in https://github.com/apache/opendal/pull/3892
* feat(services/hdfs): Atomic write for hdfs by @shbhmrzd in https://github.com/apache/opendal/pull/3875
* feat(services/hdfs): add atomic_write_dir to hdfsconfig debug by @shbhmrzd in https://github.com/apache/opendal/pull/3902
* feat: add MongodbConfig by @zjregee in https://github.com/apache/opendal/pull/3906
* RFC-3898: Concurrent Writer by @WenyXu in https://github.com/apache/opendal/pull/3898
* feat(services): add yandex disk support by @hoslo in https://github.com/apache/opendal/pull/3918
* feat: implement concurrent `MultipartUploadWriter` by @WenyXu in https://github.com/apache/opendal/pull/3915
* feat: add concurrent writer behavior tests by @WenyXu in https://github.com/apache/opendal/pull/3920
* feat: implement concurrent `RangeWriter` by @WenyXu in https://github.com/apache/opendal/pull/3923
* feat: add `concurrent` and `buffer` parameters into FuzzInput by @WenyXu in https://github.com/apache/opendal/pull/3921
* feat(fuzz): add azblob as test service by @suyanhanx in https://github.com/apache/opendal/pull/3931
* feat(services/webhdfs): Implement write with append by @hoslo in https://github.com/apache/opendal/pull/3937
* feat(core/bench): Add benchmark for concurrent write by @Xuanwo in https://github.com/apache/opendal/pull/3942
* feat(oio): add block_write support by @hoslo in https://github.com/apache/opendal/pull/3945
* feat(services/webhdfs): Implement multi write via CONCAT by @hoslo in https://github.com/apache/opendal/pull/3939
* feat(core): Allow retry in concurrent write operations by @Xuanwo in https://github.com/apache/opendal/pull/3958
* feat(services/ghac): Add workaround for AWS S3 based GHES by @Xuanwo in https://github.com/apache/opendal/pull/3985
* feat: Implement path cache and refactor gdrive by @Xuanwo in https://github.com/apache/opendal/pull/3975
* feat(services): add hdfs native layout by @shbhmrzd in https://github.com/apache/opendal/pull/3933
* feat(services/s3): Return error if credential is empty after loaded by @Xuanwo in https://github.com/apache/opendal/pull/4000
* feat(services/gdrive): Use trash instead of permanently deletes by @Xuanwo in https://github.com/apache/opendal/pull/4002
* feat(services): add koofr support by @hoslo in https://github.com/apache/opendal/pull/3981
* feat(icloud): Add basic Apple iCloud Drive support by @bokket in https://github.com/apache/opendal/pull/3980

### Changed
* refactor: Merge compose_{read,write} into enum_utils by @Xuanwo in https://github.com/apache/opendal/pull/3871
* refactor(services/ftp): Impl parse_error instead of `From<Error>` by @bokket in https://github.com/apache/opendal/pull/3891
* docs: very minor English wording fix in error message by @gabrielgrant in https://github.com/apache/opendal/pull/3900
* refactor(services/rocksdb): Impl parse_error instead of `From<Error>` by @suyanhanx in https://github.com/apache/opendal/pull/3903
* refactor: Re-organize the layout of tests by @Xuanwo in https://github.com/apache/opendal/pull/3904
* refactor(services/etcd): Impl parse_error instead of `From<Error>` by @suyanhanx in https://github.com/apache/opendal/pull/3910
* refactor(services/sftp): Impl parse_error instead of `From<Error>` by @G-XD in https://github.com/apache/opendal/pull/3914
* refactor!: Bump MSRV to 1.75 by @Xuanwo in https://github.com/apache/opendal/pull/3851
* refactor(services/redis): Impl parse_error instead of `From<Error>` by @suyanhanx in https://github.com/apache/opendal/pull/3938
* refactor!: Revert the bump of MSRV to 1.75 by @Xuanwo in https://github.com/apache/opendal/pull/3952
* refactor(services/onedrive): Add OnedriveConfig to implement ConfigDeserializer by @Borber in https://github.com/apache/opendal/pull/3954
* refactor(service/dropbox): Add DropboxConfig by @howiieyu in https://github.com/apache/opendal/pull/3961
* refactor: Polish internal types and remove not needed deps by @Xuanwo in https://github.com/apache/opendal/pull/3964
* refactor: Add concurrent error test for BlockWrite by @Xuanwo in https://github.com/apache/opendal/pull/3968
* refactor: Remove not needed types in icloud by @Xuanwo in https://github.com/apache/opendal/pull/4021

### Fixed
* fix: Bump pyo3 to fix false positive of unnecessary_fallible_conversions by @Xuanwo in https://github.com/apache/opendal/pull/3873
* fix(core): Handling content encoding correctly by @Xuanwo in https://github.com/apache/opendal/pull/3907
* fix: fix RangeWriter incorrect `next_offset` by @WenyXu in https://github.com/apache/opendal/pull/3927
* fix(oio::BlockWrite): fix write_once case by @hoslo in https://github.com/apache/opendal/pull/3953
* fix: Don't retry close if concurrent > 1 to avoid content lost by @Xuanwo in https://github.com/apache/opendal/pull/3957
* fix(doc): fix rfc typos by @howiieyu in https://github.com/apache/opendal/pull/3971
* fix: Don't call wake_by_ref in OperatorFuture by @Xuanwo in https://github.com/apache/opendal/pull/4003
* fix: async fn resumed after initiate part failed by @Xuanwo in https://github.com/apache/opendal/pull/4013
* fix(pcloud,seafile): use get_basename and get_parent by @hoslo in https://github.com/apache/opendal/pull/4020
* fix(ci): remove pr author from review candidates by @dqhl76 in https://github.com/apache/opendal/pull/4023

### Docs
* docs(bindings/python): drop unnecessary patchelf by @tisonkun in https://github.com/apache/opendal/pull/3889
* docs: Polish core's quick start by @Xuanwo in https://github.com/apache/opendal/pull/3896
* docs(gcs): correct the description of credential by @WenyXu in https://github.com/apache/opendal/pull/3928
* docs: Add 0.44.1 download link by @Xuanwo in https://github.com/apache/opendal/pull/3929
* docs(release): how to clean up old releases by @tisonkun in https://github.com/apache/opendal/pull/3934
* docs(website): polish download page by @suyanhanx in https://github.com/apache/opendal/pull/3932
* docs: improve user verify words by @tisonkun in https://github.com/apache/opendal/pull/3941
* docs: fix incorrect word used by @zegevlier in https://github.com/apache/opendal/pull/3944
* docs: improve wording for community pages by @tisonkun in https://github.com/apache/opendal/pull/3978
* docs(bindings/nodejs): copyright in footer by @suyanhanx in https://github.com/apache/opendal/pull/3986
* docs(bindings/nodejs): build docs locally doc by @suyanhanx in https://github.com/apache/opendal/pull/3987
* docs: Fix missing word in download by @Xuanwo in https://github.com/apache/opendal/pull/3993
* docs(bindings/java): copyright in footer by @G-XD in https://github.com/apache/opendal/pull/3996
* docs(website): update footer by @suyanhanx in https://github.com/apache/opendal/pull/4008
* docs: add trademark information to every possible published readme by @PsiACE in https://github.com/apache/opendal/pull/4014
* docs(website): replace podling to project in website by @morristai in https://github.com/apache/opendal/pull/4015
* docs: Update release guide to adapt as a new TLP by @Xuanwo in https://github.com/apache/opendal/pull/4011
* docs: Add WebHDFS version compatibility details by @shbhmrzd in https://github.com/apache/opendal/pull/4024

### CI
* build(deps): bump actions/download-artifact from 3 to 4 by @dependabot in https://github.com/apache/opendal/pull/3885
* build(deps): bump once_cell from 1.18.0 to 1.19.0 by @dependabot in https://github.com/apache/opendal/pull/3880
* build(deps): bump napi-derive from 2.14.2 to 2.14.6 by @dependabot in https://github.com/apache/opendal/pull/3879
* build(deps): bump url from 2.4.1 to 2.5.0 by @dependabot in https://github.com/apache/opendal/pull/3876
* build(deps): bump mlua from 0.8.10 to 0.9.2 by @oowl in https://github.com/apache/opendal/pull/3890
* ci: Disable supabase tests for our test org has been paused by @Xuanwo in https://github.com/apache/opendal/pull/3908
* ci: Downgrade artifact actions until regression addressed by @Xuanwo in https://github.com/apache/opendal/pull/3935
* ci: Refactor fuzz to integrate with test planner by @Xuanwo in https://github.com/apache/opendal/pull/3936
* ci: Pick random reviewers from committer list by @Xuanwo in https://github.com/apache/opendal/pull/4001

### Chore
* chore: update release related docs and script by @dqhl76 in https://github.com/apache/opendal/pull/3870
* chore(NOTICE): update copyright to year 2024 by @suyanhanx in https://github.com/apache/opendal/pull/3894
* chore: Format code to make readers happy by @Xuanwo in https://github.com/apache/opendal/pull/3912
* chore: Remove unused dep async-compat by @Xuanwo in https://github.com/apache/opendal/pull/3947
* chore: display project logo on Rust docs by @tisonkun in https://github.com/apache/opendal/pull/3983
* chore: improve trademarks in bindings docs by @tisonkun in https://github.com/apache/opendal/pull/3984
* chore: use Apache OpenDAL™ in the first and most prominent mention by @tisonkun in https://github.com/apache/opendal/pull/3988
* chore: add more information for javadocs by @tisonkun in https://github.com/apache/opendal/pull/3989
* chore: precise footer by @tisonkun in https://github.com/apache/opendal/pull/3997
* chore: use full form name when necessary by @tisonkun in https://github.com/apache/opendal/pull/3998
* chore(bindings/python): Enable sftp service by default for unix platform by @Zheaoli in https://github.com/apache/opendal/pull/4006
* chore: remove disclaimer by @suyanhanx in https://github.com/apache/opendal/pull/4009
* chore: Remove incubating from releases by @Xuanwo in https://github.com/apache/opendal/pull/4010
* chore: trim incubator prefix everywhere by @tisonkun in https://github.com/apache/opendal/pull/4016
* chore: fixup doc link in release_java.yml by @tisonkun in https://github.com/apache/opendal/pull/4019
* chore: simplify reviewer candidates logic by @tisonkun in https://github.com/apache/opendal/pull/4017

## [v0.44.1] - 2023-12-31

### Added
* feat(service/memcached): Add MemCachedConfig by @ankit-pn in https://github.com/apache/opendal/pull/3827
* feat(service/rocksdb): Add RocksdbConfig by @ankit-pn in https://github.com/apache/opendal/pull/3828
* feat(services): add chainsafe support by @hoslo in https://github.com/apache/opendal/pull/3834
* feat(bindings/python): Build all available services for python by @Xuanwo in https://github.com/apache/opendal/pull/3836
* feat: Adding Atomicserver config by @k-aishwarya in https://github.com/apache/opendal/pull/3845
* feat(oio::read): implement the async buffer reader by @WenyXu in https://github.com/apache/opendal/pull/3811
* feat(oio::read): implement the blocking buffer reader by @WenyXu in https://github.com/apache/opendal/pull/3860
* feat: adapt the `CompleteReader` by @WenyXu in https://github.com/apache/opendal/pull/3861
* feat: add basic behavior tests for buffer reader by @WenyXu in https://github.com/apache/opendal/pull/3862
* feat: add fuzz reader with buffer tests by @WenyXu in https://github.com/apache/opendal/pull/3866
* feat(ofs): implement ofs based on fuse3 by @Inokinoki in https://github.com/apache/opendal/pull/3857
### Changed
* refactor: simplify `bindings_python.yml` by @messense in https://github.com/apache/opendal/pull/3837
* refactor: Add edge test for aws assume role with web identity by @Xuanwo in https://github.com/apache/opendal/pull/3839
* refactor(services/webdav): Add WebdavConfig to implement ConfigDeserializer by @kwaa in https://github.com/apache/opendal/pull/3846
* refactor: use TwoWays instead of TwoWaysReader and TwoWaysWriter by @WenyXu in https://github.com/apache/opendal/pull/3863
### Fixed
* fix: Add tests for listing recursively on not supported services by @Xuanwo in https://github.com/apache/opendal/pull/3826
* fix(services/upyun): fix list api by @hoslo in https://github.com/apache/opendal/pull/3841
* fix: fix a bypass seek relative bug in `BufferReader` by @WenyXu in https://github.com/apache/opendal/pull/3864
* fix: fix the bypass read does not sync the `cur` of `BufferReader` by @WenyXu in https://github.com/apache/opendal/pull/3865
### Docs
* docs: Add Apache prefix for all bindings by @Xuanwo in https://github.com/apache/opendal/pull/3829
* docs: Add apache prefix for python docs by @Xuanwo in https://github.com/apache/opendal/pull/3830
* docs: Add branding in README by @Xuanwo in https://github.com/apache/opendal/pull/3831
* docs: Add trademark for Apache OpenDAL™ by @Xuanwo in https://github.com/apache/opendal/pull/3832
* docs: Add trademark sign for core by @Xuanwo in https://github.com/apache/opendal/pull/3833
* docs: Enable doc_auto_cfg when docs cfg has been enabled by @Xuanwo in https://github.com/apache/opendal/pull/3835
* docs: Address branding for haskell and C bindings by @Xuanwo in https://github.com/apache/opendal/pull/3840
* doc: add 0.44.0 release link to download.md by @dqhl76 in https://github.com/apache/opendal/pull/3868
### CI
* ci: Remove workflows that not running or ready by @Xuanwo in https://github.com/apache/opendal/pull/3842
* ci: Migrate ftp to test planner by @Xuanwo in https://github.com/apache/opendal/pull/3843
### Chore
* chore(bindings/java): Add name and description metadata by @tisonkun in https://github.com/apache/opendal/pull/3838
* chore(website): improve a bit trademark refs by @tisonkun in https://github.com/apache/opendal/pull/3847
* chore: Fix clippy warnings found in rust 1.75 by @Xuanwo in https://github.com/apache/opendal/pull/3849
* chore(bindings/python): improve ASF branding by @tisonkun in https://github.com/apache/opendal/pull/3850
* chore(bindings/haskell): improve ASF branding by @tisonkun in https://github.com/apache/opendal/pull/3852
* chore(bindings/c): make c binding separate workspace by @suyanhanx in https://github.com/apache/opendal/pull/3856
* chore(bindings/haskell): support co-log-0.6.0 && ghc-9.4 by @silver-ymz in https://github.com/apache/opendal/pull/3858


## [v0.44.0] - 2023-12-26

### Added
* feat(core): service add HuggingFace file system by @morristai in https://github.com/apache/opendal/pull/3670
* feat(service/moka): bump moka from 0.10.4 to 0.12.1 by @G-XD in https://github.com/apache/opendal/pull/3711
* feat: add operator.list support for OCaml binding by @Young-Flash in https://github.com/apache/opendal/pull/3706
* feat(test): add Huggingface behavior test by @morristai in https://github.com/apache/opendal/pull/3712
* feat: Add list prefix support by @Xuanwo in https://github.com/apache/opendal/pull/3728
* feat: Implement ConcurrentFutures to remove the dependences on tokio by @Xuanwo in https://github.com/apache/opendal/pull/3746
* feat(services): add seafile support by @hoslo in https://github.com/apache/opendal/pull/3771
* RFC-3734: Buffered reader  by @WenyXu in https://github.com/apache/opendal/pull/3734
* feat: Add content range support for RpRead by @Xuanwo in https://github.com/apache/opendal/pull/3777
* feat: Add presign_stat_with support by @Xuanwo in https://github.com/apache/opendal/pull/3778
* feat: Add project layout for ofs by @Xuanwo in https://github.com/apache/opendal/pull/3779
* feat(binding/nodejs): align list api by @suyanhanx in https://github.com/apache/opendal/pull/3784
* feat: Make OpenDAL available under wasm32 arch by @Xuanwo in https://github.com/apache/opendal/pull/3796
* feat(services): add upyun support by @hoslo in https://github.com/apache/opendal/pull/3790
* feat: Add edge test s3_read_on_wasm by @Xuanwo in https://github.com/apache/opendal/pull/3802
* feat(services/azblob): available under wasm32 arch by @suyanhanx in https://github.com/apache/opendal/pull/3806
* feat: make services-gdrive compile for wasm target by @Young-Flash in https://github.com/apache/opendal/pull/3808
* feat(core): Make gcs available on wasm32 arch by @Xuanwo in https://github.com/apache/opendal/pull/3816
### Changed
* refactor(service/etcd): use EtcdConfig in from_map by @G-XD in https://github.com/apache/opendal/pull/3703
* refactor(object_store): upgrade object_store to 0.7. by @youngsofun in https://github.com/apache/opendal/pull/3713
* refactor: List must support list without recursive by @Xuanwo in https://github.com/apache/opendal/pull/3721
* refactor: replace ftp tls impl as rustls by @oowl in https://github.com/apache/opendal/pull/3760
* refactor: Remove never used Stream poll_reset API by @Xuanwo in https://github.com/apache/opendal/pull/3774
* refactor: Polish operator read_with by @Xuanwo in https://github.com/apache/opendal/pull/3775
* refactor: Migrate gcs builder to config based by @Xuanwo in https://github.com/apache/opendal/pull/3786
* refactor(service/hdfs): Add HdfsConfig to implement ConfigDeserializer by @shbhmrzd in https://github.com/apache/opendal/pull/3800
* refactor(raw): add parse_header_to_str fn by @hoslo in https://github.com/apache/opendal/pull/3804
* refactor(raw): refactor APIs like parse_content_disposition by @hoslo in https://github.com/apache/opendal/pull/3815
* refactor: Polish http_util parse headers by @Xuanwo in https://github.com/apache/opendal/pull/3817
### Fixed
* fix(oli): Fix cp -r command returns invalid path error by @kebe7jun in https://github.com/apache/opendal/pull/3687
* fix(website): folder name mismatch by @suyanhanx in https://github.com/apache/opendal/pull/3707
* fix(binding/java): fix SPECIAL_DIR_NAME by @G-XD in https://github.com/apache/opendal/pull/3715
* fix(services/dropbox): Workaround for dropbox limitations for create_folder by @Xuanwo in https://github.com/apache/opendal/pull/3719
* fix(ocaml_binding): sort `actual` & `expected` to pass ci by @Young-Flash in https://github.com/apache/opendal/pull/3733
* fix(ci): Make sure merge_local_staging handles all subdir by @Xuanwo in https://github.com/apache/opendal/pull/3788
* fix(services/gdrive): fix return value of `get_file_id_by_path` by @G-XD in https://github.com/apache/opendal/pull/3801
* fix(core): List root should not return itself by @Xuanwo in https://github.com/apache/opendal/pull/3824
### Docs
* docs: add maturity model check by @suyanhanx in https://github.com/apache/opendal/pull/3680
* docs(website): show maturity model by @suyanhanx in https://github.com/apache/opendal/pull/3709
* docs(website): only VOTEs from PPMC members are binding by @G-XD in https://github.com/apache/opendal/pull/3710
* doc: add 0.43.0 release link to download.md by @G-XD in https://github.com/apache/opendal/pull/3729
* docs: Add process on nominating committers and ppmc members by @Xuanwo in https://github.com/apache/opendal/pull/3740
* docs: Deploy website to nightlies for every tags by @Xuanwo in https://github.com/apache/opendal/pull/3739
* docs: Remove not released bindings docs from top level header by @Xuanwo in https://github.com/apache/opendal/pull/3741
* docs: Add dependencies list for all packages by @Xuanwo in https://github.com/apache/opendal/pull/3743
* docs: Update maturity docs by @Xuanwo in https://github.com/apache/opendal/pull/3750
* docs: update the RFC doc by @suyanhanx in https://github.com/apache/opendal/pull/3748
* docs(website): polish deploy to nightlies by @suyanhanx in https://github.com/apache/opendal/pull/3753
* docs: add event calendar in community page by @dqhl76 in https://github.com/apache/opendal/pull/3767
* docs(community): polish events by @suyanhanx in https://github.com/apache/opendal/pull/3768
* docs(bindings/ruby): reflect test framework refactor by @tisonkun in https://github.com/apache/opendal/pull/3798
* docs(website): add service Huggingface to website by @morristai in https://github.com/apache/opendal/pull/3812
* docs: update release docs to add cargo-deny setup by @dqhl76 in https://github.com/apache/opendal/pull/3821
### CI
* build(deps): bump cacache from 11.7.1 to 12.0.0 by @dependabot in https://github.com/apache/opendal/pull/3690
* build(deps): bump prometheus-client from 0.21.2 to 0.22.0 by @dependabot in https://github.com/apache/opendal/pull/3694
* build(deps): bump github/issue-labeler from 3.2 to 3.3 by @dependabot in https://github.com/apache/opendal/pull/3698
* ci: Add behavior test for b2 by @Xuanwo in https://github.com/apache/opendal/pull/3714
* ci(cargo): Add frame pointer support in build flag by @Zheaoli in https://github.com/apache/opendal/pull/3772
* ci: Workaround ring 0.17 build issue, bring aarch64 and armv7l back by @Xuanwo in https://github.com/apache/opendal/pull/3781
* ci: Support CI test for s3_read_on_wasm by @Zheaoli in https://github.com/apache/opendal/pull/3813
### Chore
* chore: bump aws-sdk-s3 from 0.38.0 to 1.4.0 by @memoryFade in https://github.com/apache/opendal/pull/3704
* chore: Disable obs test for workaround by @Xuanwo in https://github.com/apache/opendal/pull/3717
* chore: Fix bindings CI by @Xuanwo in https://github.com/apache/opendal/pull/3722
* chore(binding/nodejs,website): Replace yarn with pnpm by @suyanhanx in https://github.com/apache/opendal/pull/3730
* chore: Bring persy CI back by @Xuanwo in https://github.com/apache/opendal/pull/3751
* chore(bindings/python): upgrade pyo3 to 0.20 by @messense in https://github.com/apache/opendal/pull/3758
* chore: remove unused binding feature file by @tisonkun in https://github.com/apache/opendal/pull/3757
* chore: Bump governor from 0.5.1 to 0.6.0 by @G-XD in https://github.com/apache/opendal/pull/3761
* chore: Split bindings/ocaml to separate workspace by @Xuanwo in https://github.com/apache/opendal/pull/3792
* chore: Split bindings/ruby to separate workspace by @ho-229 in https://github.com/apache/opendal/pull/3794
* chore(bindings/php): bump ext-php-rs to support latest php & separate workspace by @suyanhanx in https://github.com/apache/opendal/pull/3799
* chore: Address comments from hackernews by @Xuanwo in https://github.com/apache/opendal/pull/3805
* chore(bindings/ocaml): dep opendal point to core by @suyanhanx in https://github.com/apache/opendal/pull/3814

## [v0.43.0] - 2023-11-30

### Added
* feat(bindings/C): Add opendal_operator_rename and opendal_operator_copy by @jiaoew1991 in https://github.com/apache/opendal/pull/3517
* feat(binding/python): Add new API to convert between AsyncOperator and Operator by @Zheaoli in https://github.com/apache/opendal/pull/3514
* feat: Implement RFC-3526: List Recursive by @Xuanwo in https://github.com/apache/opendal/pull/3556
* feat(service): add alluxio rest api support by @hoslo in https://github.com/apache/opendal/pull/3564
* feat(bindings/python): add OPENDAL_DISABLE_RANDOM_ROOT support by @Justin-Xiang in https://github.com/apache/opendal/pull/3550
* feat(core): add Alluxio e2e test by @hoslo in https://github.com/apache/opendal/pull/3573
* feat(service): alluxio support write by @hoslo in https://github.com/apache/opendal/pull/3566
* feat(bindings/nodejs): add retry layer by @suyanhanx in https://github.com/apache/opendal/pull/3484
* RFC: Concurrent Stat in List by @morristai in https://github.com/apache/opendal/pull/3574
* feat(service/hdfs):  enable rename in hdfs service by @qingwen220 in https://github.com/apache/opendal/pull/3592
* feat: Improve the read_to_end perf and add benchmark vs_fs by @Xuanwo in https://github.com/apache/opendal/pull/3617
* feat: Add benchmark vs aws sdk s3 by @Xuanwo in https://github.com/apache/opendal/pull/3620
* feat: Improve the performance of s3 services by @Xuanwo in https://github.com/apache/opendal/pull/3622
* feat(service): support b2 by @hoslo in https://github.com/apache/opendal/pull/3604
* feat(core): Implement RFC-3574 Concurrent Stat In List by @morristai in https://github.com/apache/opendal/pull/3599
* feat: Implement stat dir correctly based on RFC-3243 List Prefix by @Xuanwo in https://github.com/apache/opendal/pull/3651
* feat(bindings/nodejs): Add capability support by @suyanhanx in https://github.com/apache/opendal/pull/3654
* feat: disable `ftp` for python and java binding by @ZutJoe in https://github.com/apache/opendal/pull/3659
* feat(bindings/nodejs): read/write stream by @suyanhanx in https://github.com/apache/opendal/pull/3619
### Changed
* refactor(services/persy): migrate tot test planner by @G-XD in https://github.com/apache/opendal/pull/3476
* refactor(service/etcd): Add EtcdConfig to implement ConfigDeserializer by @Xuxiaotuan in https://github.com/apache/opendal/pull/3543
* refactor(services/azblob): add AzblobConfig by @acehinnnqru in https://github.com/apache/opendal/pull/3553
* refactor(services/cacache): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3568
* refactor(services/sled): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3569
* refactor(services/webhdfs): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3578
* refactor(core): Rename all `Page` to `List` by @Xuanwo in https://github.com/apache/opendal/pull/3589
* refactor: Change List API into poll based and return one entry instead by @Xuanwo in https://github.com/apache/opendal/pull/3593
* refactor(services/tikv): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3587
* refactor(service/redis): Migrate task to new task planner by @sunheyi6 in https://github.com/apache/opendal/pull/3374
* refactor(oio): Polish IncomingAsyncBody::bytes by @Xuanwo in https://github.com/apache/opendal/pull/3621
* refactor(services/rocksdb): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3636
* refactor(services/azfile): Check if dir exists before create by @ZutJoe in https://github.com/apache/opendal/pull/3652
* refactor: Polish concurrent list by @Xuanwo in https://github.com/apache/opendal/pull/3658
### Fixed
* fix(bindings/python): Fix the test command in doc by @Justin-Xiang in https://github.com/apache/opendal/pull/3541
* fix(ci): try enable corepack before setup-node action by @suyanhanx in https://github.com/apache/opendal/pull/3609
* fix(service/hdfs): enable hdfs append support by @qingwen220 in https://github.com/apache/opendal/pull/3600
* fix(ci): fix setup node by @suyanhanx in https://github.com/apache/opendal/pull/3611
* fix(core): Path in remove not normalized by @Xuanwo in https://github.com/apache/opendal/pull/3671
* fix(core): Build with redis features and Rust < 1.72 by @vincentdephily in https://github.com/apache/opendal/pull/3683
### Docs
* docs: Add questdb in users list by @caicancai in https://github.com/apache/opendal/pull/3532
* docs: Add macos specific doc updates for hdfs by @shbhmrzd in https://github.com/apache/opendal/pull/3559
* docs(bindings/python): Fix the test command in doc by @Justin-Xiang in https://github.com/apache/opendal/pull/3561
* docs(bindings/java): add basic usage in README by @caicancai in https://github.com/apache/opendal/pull/3534
* doc: add 0.42.0 release link to download.md by @silver-ymz in https://github.com/apache/opendal/pull/3598
### CI
* ci(services/libsql): add rust test threads limit by @G-XD in https://github.com/apache/opendal/pull/3540
* ci(services/redb): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3518
* ci: Disable libsql behavior test until we or upstream address them by @Xuanwo in https://github.com/apache/opendal/pull/3552
* ci: Add new Python binding reviewer by @Zheaoli in https://github.com/apache/opendal/pull/3560
* ci(bindings/nodejs): add aarch64 build support by @suyanhanx in https://github.com/apache/opendal/pull/3567
* ci(planner): Polish the workflow planner code by @Zheaoli in https://github.com/apache/opendal/pull/3570
* ci(core): Add dry run for rc tags by @Xuanwo in https://github.com/apache/opendal/pull/3624
* ci: Disable persy until it has been fixed by @Xuanwo in https://github.com/apache/opendal/pull/3631
* ci: Calling cargo to make sure rust has been setup by @Xuanwo in https://github.com/apache/opendal/pull/3633
* ci: Fix etcd with tls and auth failed to start by @Xuanwo in https://github.com/apache/opendal/pull/3637
* ci(services/etcd): Use ALLOW_NONE_AUTHENTICATION as workaround by @Xuanwo in https://github.com/apache/opendal/pull/3638
* ci: dry run publish on rc tags for python binding by @everpcpc in https://github.com/apache/opendal/pull/3645
* ci: Add java linux arm64 build by @Xuanwo in https://github.com/apache/opendal/pull/3660
* ci(java/binding): Use zigbuild for glibc 2.17 support by @Xuanwo in https://github.com/apache/opendal/pull/3664
* ci(bindings/python): remove aarch support by @G-XD in https://github.com/apache/opendal/pull/3674
### Chore
* chore(servies/sftp): Upgrade openssh-sftp-client to 0.14 by @sd44 in https://github.com/apache/opendal/pull/3538
* chore(service/tikv): rename Backend to TikvBackend by @caicancai in https://github.com/apache/opendal/pull/3545
* chore(docs): add cpp binding in README by @cjj2010 in https://github.com/apache/opendal/pull/3546
* chore(service/sqlite): fix typo on sqlite by @caicancai in https://github.com/apache/opendal/pull/3549
* chore(bindings/C): resolve doxygen warnings by @sd44 in https://github.com/apache/opendal/pull/3572
* chore: removed dotenv in bindings/nodejs/index.js by @AlexVCS in https://github.com/apache/opendal/pull/3579
* chore: update opentelemetry to v0.21.x by @jtescher in https://github.com/apache/opendal/pull/3580
* chore: Add cpp binding Google style clang-format && format the code by @JackDrogon in https://github.com/apache/opendal/pull/3581
* chore: bump suppaftp version to 5.2 by @oowl in https://github.com/apache/opendal/pull/3590
* chore(ci): fix artifacts path for publish pypi by @everpcpc in https://github.com/apache/opendal/pull/3597
* chore: Code cleanup to make rust 1.74 happy by @Xuanwo in https://github.com/apache/opendal/pull/3602
* chore: Fix `raw::tests` been excluded unexpectedly by @Xuanwo in https://github.com/apache/opendal/pull/3623
* chore: Bump dpes and remove native-tls in mysql-async by @Xuanwo in https://github.com/apache/opendal/pull/3627
* chore(core): Have mysql_async use rustls instead of native-tls by @amunra in https://github.com/apache/opendal/pull/3634
* chore: Polish docs for Capability by @Xuanwo in https://github.com/apache/opendal/pull/3635
* chore: Bump reqsign to 0.14.4 for jsonwebtoken by @Xuanwo in https://github.com/apache/opendal/pull/3644
* chore(ci): nodejs binding publish dry run by @suyanhanx in https://github.com/apache/opendal/pull/3632
* chore: Polish comments for `stat` and `stat_with` by @Xuanwo in https://github.com/apache/opendal/pull/3657
* chore: clearer doc for python binding by @wcy-fdu in https://github.com/apache/opendal/pull/3667
* chore: Bump to v0.43.0 to start release process by @G-XD in https://github.com/apache/opendal/pull/3672
* chore: Bump to v0.43.0 to start release process (Round 2) by @G-XD in https://github.com/apache/opendal/pull/3676
* chore: add license.workspace to help cargo deny reports by @tisonkun in https://github.com/apache/opendal/pull/3679
* chore: clearer doc for list metakey by @wcy-fdu in https://github.com/apache/opendal/pull/3666

## [v0.42.0] - 2023-11-07

### Added
* feat(binding/java): add `rename` support by @G-XD in https://github.com/apache/opendal/pull/3238
* feat(prometheus): add bytes metrics as counter by @flaneur2020 in https://github.com/apache/opendal/pull/3246
* feat(binding/python): new behavior testing for python by @laipz8200 in https://github.com/apache/opendal/pull/3245
* feat(binding/python): Support AsyncOperator tests. by @laipz8200 in https://github.com/apache/opendal/pull/3254
* feat(service/libsql): support libsql by @G-XD in https://github.com/apache/opendal/pull/3233
* feat(binding/python): allow setting append/buffer/more in write() call by @jokester in https://github.com/apache/opendal/pull/3256
* feat(services/persy): change blocking_x in async_x call to tokio::task::blocking_spawn by @Zheaoli in https://github.com/apache/opendal/pull/3221
* feat: Add edge test cases for OpenDAL Core by @Xuanwo in https://github.com/apache/opendal/pull/3274
* feat(service/d1): Support d1 for opendal by @realtaobo in https://github.com/apache/opendal/pull/3248
* feat(services/redb): change blocking_x in async_x call to tokio::task::blocking_spawn by @shauvet in https://github.com/apache/opendal/pull/3276
* feat: Add blocking layer for C bindings by @jiaoew1991 in https://github.com/apache/opendal/pull/3278
* feat(binding/c): Add blocking_reader for C binding by @jiaoew1991 in https://github.com/apache/opendal/pull/3259
* feat(services/sled): change blocking_x in async_x call to tokio::task::blocking_spawn by @shauvet in https://github.com/apache/opendal/pull/3280
* feat(services/rocksdb): change blocking_x in async_x call to tokio::task::blocking_spawn by @shauvet in https://github.com/apache/opendal/pull/3279
* feat(binding/java): make `Metadata` a POJO by @G-XD in https://github.com/apache/opendal/pull/3277
* feat(bindings/java): convey backtrace on exception by @tisonkun in https://github.com/apache/opendal/pull/3286
* feat(layer/prometheus): Support custom metric bucket for Histogram by @Zheaoli in https://github.com/apache/opendal/pull/3275
* feat(bindings/python): read APIs return `memoryview` instead of `bytes` to avoid copy by @messense in https://github.com/apache/opendal/pull/3310
* feat(service/azfile): add azure file service support by @dqhl76 in https://github.com/apache/opendal/pull/3312
* feat(services/oss): Add allow anonymous support by @Xuanwo in https://github.com/apache/opendal/pull/3321
* feat(bindings/python): build and publish aarch64 and armv7l wheels by @messense in https://github.com/apache/opendal/pull/3325
* feat(bindings/java): support duplicate operator by @tisonkun in https://github.com/apache/opendal/pull/3330
* feat(core): Add enabled for Scheme by @Xuanwo in https://github.com/apache/opendal/pull/3331
* feat(bindings/java): support get enabled services by @tisonkun in https://github.com/apache/opendal/pull/3336
* feat(bindings/java): Migrate behavior tests to new Workflow Planner by @Xuanwo in https://github.com/apache/opendal/pull/3341
* feat(layer/prometheus): Support output path as a metric label by @Zheaoli in https://github.com/apache/opendal/pull/3335
* feat(service/mongodb): Support mongodb service by @Zheaoli in https://github.com/apache/opendal/pull/3355
* feat: Make PrometheusClientLayer Clonable by @flaneur2020 in https://github.com/apache/opendal/pull/3352
* feat(service/cloudflare_kv): support cloudflare KV by @my-vegetable-has-exploded in https://github.com/apache/opendal/pull/3348
* feat(core): exposing `Metadata::metakey()` api by @G-XD in https://github.com/apache/opendal/pull/3373
* feat(binding/java): add list & remove_all support by @G-XD in https://github.com/apache/opendal/pull/3333
* feat: Add write_total_max_size in Capability by @realtaobo in https://github.com/apache/opendal/pull/3309
* feat(core): service add DBFS API 2.0 support by @morristai in https://github.com/apache/opendal/pull/3334
* feat(bindings/java): use random root for behavior tests by @tisonkun in https://github.com/apache/opendal/pull/3408
* feat(services/oss): Add start-after support for oss list by @wcy-fdu in https://github.com/apache/opendal/pull/3410
* feat(binding/python): Export full_capability API for Python binding by @Zheaoli in https://github.com/apache/opendal/pull/3402
* feat(test): Enable new test workflow planner for python binding by @Zheaoli in https://github.com/apache/opendal/pull/3397
* feat: Implement Lazy Reader by @Xuanwo in https://github.com/apache/opendal/pull/3395
* feat(binding/nodejs): upgrade test behavior and infra by @eryue0220 in https://github.com/apache/opendal/pull/3297
* feat(binding/python): Support Copy operation for Python binding by @Zheaoli in https://github.com/apache/opendal/pull/3454
* feat(bindings/python): Add layer API for operator by @Xuanwo in https://github.com/apache/opendal/pull/3464
* feat(bindings/java): add layers onto ops by @tisonkun in https://github.com/apache/opendal/pull/3392
* feat(binding/python): Support rename API for Python binding by @Zheaoli in https://github.com/apache/opendal/pull/3467
* feat(binding/python): Support remove_all API for Python binding by @Zheaoli in https://github.com/apache/opendal/pull/3469
* feat(core): fix token leak in OneDrive by @morristai in https://github.com/apache/opendal/pull/3470
* feat(core): service add OpenStack Swift support by @morristai in https://github.com/apache/opendal/pull/3461
* feat(bindings/python)!: Implement File and AsyncFile to replace Reader by @Xuanwo in https://github.com/apache/opendal/pull/3474
* feat(services): Implement ConfigDeserializer and add S3Config as example by @Xuanwo in https://github.com/apache/opendal/pull/3490
* feat(core): add OpenStack Swift e2e test by @morristai in https://github.com/apache/opendal/pull/3493
* feat(doc): add OpenStack Swift document for the website by @morristai in https://github.com/apache/opendal/pull/3494
* feat(services/sqlite): add SqliteConfig by @hoslo in https://github.com/apache/opendal/pull/3497
* feat(bindings/C): implement capability by @Ji-Xinyou in https://github.com/apache/opendal/pull/3479
* feat: add mongodb gridfs service support by @realtaobo in https://github.com/apache/opendal/pull/3491
* feat(services): add RedisConfig by @hoslo in https://github.com/apache/opendal/pull/3498
* feat: Add opendal_metadata_last_modified and opendal_operator_create_dir by @jiaoew1991 in https://github.com/apache/opendal/pull/3515
### Changed
* refactor(services/sqlite): Polish sqlite via adding connection pool by @Xuanwo in https://github.com/apache/opendal/pull/3249
* refactor: Remove cucumber based test in python by @laipz8200 in https://github.com/apache/opendal/pull/3253
* refactor: Introduce OpenDAL Workflow Planner by @Xuanwo in https://github.com/apache/opendal/pull/3258
* refactor(bindings/C): Implement error with error message by @Ji-Xinyou in https://github.com/apache/opendal/pull/3250
* refactor(oay): import dav-server-opendalfs by @Young-Flash in https://github.com/apache/opendal/pull/3285
* refactor(bindings/java): explicit error handling by @tisonkun in https://github.com/apache/opendal/pull/3288
* refactor(services/gdrive): Extract folder search logic by @Xuanwo in https://github.com/apache/opendal/pull/3234
* refactor(core): use `list_with` in `Operator::list` by @G-XD in https://github.com/apache/opendal/pull/3305
* refactor(!): Bump and update MSRV to 1.67 by @Xuanwo in https://github.com/apache/opendal/pull/3316
* refactor(tests): Apply OPENDAL_TEST for behavior test by @Xuanwo in https://github.com/apache/opendal/pull/3322
* refactor(bindings/java): align test idiom with OPENDAL_TEST by @tisonkun in https://github.com/apache/opendal/pull/3328
* refactor(bindings/java): split behavior tests by @tisonkun in https://github.com/apache/opendal/pull/3332
* refactor(ci/behavior_test): Migrate to 1password instead by @Xuanwo in https://github.com/apache/opendal/pull/3338
* refactor(core/{fuzz,benches}): Migrate to OPENDANL_TEST by @Xuanwo in https://github.com/apache/opendal/pull/3343
* refactor(bindings/C): Alter naming convention for consistency by @Ji-Xinyou in https://github.com/apache/opendal/pull/3282
* refactor(service/mysql): Migrate to new task planner by @Zheaoli in https://github.com/apache/opendal/pull/3357
* refactor(service/postgresql): Migrate task to new task planner by @Zheaoli in https://github.com/apache/opendal/pull/3358
* refactor(services/etcd): Migrate etcd task to new behavior test planner by @Zheaoli in https://github.com/apache/opendal/pull/3360
* refactor(services/http): Migrate http task to new behavior test planner by @Zheaoli in https://github.com/apache/opendal/pull/3362
* refactor(services/sqlite): Migrate sqlite task to new behavior test planner by @Zheaoli in https://github.com/apache/opendal/pull/3365
* refactor(services/gdrive): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3368
* refactor(services/redis): migrate to test planner for kvrocks,dragonfly by @suyanhanx in https://github.com/apache/opendal/pull/3369
* refactor(services/azblob): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3370
* refactor(services/cos,obs): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3371
* refactor(services/oss): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3375
* refactor(services/memcached): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3377
* refactor(services/gcs): migrate tot test planner by @suyanhanx in https://github.com/apache/opendal/pull/3391
* refactor(services/moka): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3394
* refactor(services/dashmap): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3396
* refactor(services/memory): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3390
* refactor(services/azdls): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3405
* refactor(services/mini_moka): migrate to test planner by @dqhl76 in https://github.com/apache/opendal/pull/3416
* refactor(core/fuzz): Fix some bugs inside fuzzer by @Xuanwo in https://github.com/apache/opendal/pull/3418
* refactor(tests): Extract tests related logic into raw::tests for reuse by @Xuanwo in https://github.com/apache/opendal/pull/3420
* refactor(service/dropbox): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3381
* refactor(services/supabase): migrate to test planner by @G-XD in https://github.com/apache/opendal/pull/3406
* refactor(services/sftp): migrate to test planner by @suyanhanx in https://github.com/apache/opendal/pull/3412
* refactor(services/wasabi)!: Remove native support for wasabi services by @Xuanwo in https://github.com/apache/opendal/pull/3455
* refactor(ci): Polish the test planner code by @Zheaoli in https://github.com/apache/opendal/pull/3457
* refactor(services/webdav): migrate to test planner for webdav by @shauvet in https://github.com/apache/opendal/pull/3379
* refactor(services/redis): Enable rustls support by default for redis by @Xuanwo in https://github.com/apache/opendal/pull/3471
* refactor(bindings/python): Refactor layout for python bindings by @Xuanwo in https://github.com/apache/opendal/pull/3473
* refactor(services/libsql): Migrate libsql task to new behavior test planner by @Zheaoli in https://github.com/apache/opendal/pull/3363
* refactor(service/postgresql): Add PostgresqlConfig to implement ConfigDeserializer by @sd44 in https://github.com/apache/opendal/pull/3495
* refactor(binding/python): Add multiple custom exception for each of error code in Rust Core by @Zheaoli in https://github.com/apache/opendal/pull/3492
* refactor(service/libsql): Add LibsqlConfig to implement ConfigDeserializer by @sd44 in https://github.com/apache/opendal/pull/3501
* refactor(service/http): Add HttpConfig to implement ConfigDeserializer by @sd44 in https://github.com/apache/opendal/pull/3507
* refactor(service/ftp): Add FtpConfig to implement ConfigDeserializer by @sd44 in https://github.com/apache/opendal/pull/3510
* refactor(service/sftp): Add SftpConfig to implement ConfigDeserializer by @sd44 in https://github.com/apache/opendal/pull/3511
* refactor(service/tikv): Add TikvConfig to implement ConfigDeserializer by @caicancai in https://github.com/apache/opendal/pull/3512
### Fixed
* fix: Fix read result not full by @jiaoew1991 in https://github.com/apache/opendal/pull/3350
* fix(services/cos): fix prefix param by @G-XD in https://github.com/apache/opendal/pull/3384
* fix(services/ghac)!: Remove enable_create_simulation support for ghac by @Xuanwo in https://github.com/apache/opendal/pull/3423
* fix: ASF event URL by @tisonkun in https://github.com/apache/opendal/pull/3431
* fix(binding/java): fix return value of presign-related method by @G-XD in https://github.com/apache/opendal/pull/3433
* fix(mongo/backend): remove redundant code by @bestgopher in https://github.com/apache/opendal/pull/3439
* fix: nodejs test adapt `OPENDAL_DISABLE_RANDOM_ROOT` by @suyanhanx in https://github.com/apache/opendal/pull/3456
* fix(services/s3): Accept List responses without ETag by @amunra in https://github.com/apache/opendal/pull/3478
* fix(bindings/python): fix type annotations and improve docs by @messense in https://github.com/apache/opendal/pull/3483
* fix(services/dropbox): Check if folder exists before calling create dir by @leenstx in https://github.com/apache/opendal/pull/3513
### Docs
* docs: Add docs in website for sqlite/mysql/postgresql services by @Zheaoli in https://github.com/apache/opendal/pull/3290
* docs: add docs in website for atomicserver by @Zheaoli in https://github.com/apache/opendal/pull/3293
* docs: Add docs on website for GHAC service by @Zheaoli in https://github.com/apache/opendal/pull/3296
* docs: Add docs on website for cacache services by @Zheaoli in https://github.com/apache/opendal/pull/3294
* docs: Add docs on website for libsql services by @Zheaoli in https://github.com/apache/opendal/pull/3299
* docs: download link for v0.41.0 by @suyanhanx in https://github.com/apache/opendal/pull/3298
* docs: Add docs on website for persy service by @Zheaoli in https://github.com/apache/opendal/pull/3300
* docs: Add docs on website for d1 services by @Zheaoli in https://github.com/apache/opendal/pull/3295
* docs: Add docs on website for redb service by @Zheaoli in https://github.com/apache/opendal/pull/3301
* docs: Add docs on website for tikv service by @Zheaoli in https://github.com/apache/opendal/pull/3302
* docs: Add docs on website for Vercel Artifacts service by @Zheaoli in https://github.com/apache/opendal/pull/3303
* docs: update release doc by @suyanhanx in https://github.com/apache/opendal/pull/3306
* docs(bindings): bindings README and binding release status by @suyanhanx in https://github.com/apache/opendal/pull/3340
* docs(bindings/java): update how to run behavior test by @tisonkun in https://github.com/apache/opendal/pull/3342
* docs: fix something in docs by @my-vegetable-has-exploded in https://github.com/apache/opendal/pull/3353
* docs: Update mysql `connection_string` config description in doc by @xring in https://github.com/apache/opendal/pull/3388
* doc: apply `range_reader` change in upgrade doc  by @wcy-fdu in https://github.com/apache/opendal/pull/3401
* docs(readme): Fix capitalization about the ABFS service in README.md by @caicancai in https://github.com/apache/opendal/pull/3485
* docs: Add Milvus as C binding's user by @Xuanwo in https://github.com/apache/opendal/pull/3523
### CI
* ci: Add bindings_go workflow by @jiaoew1991 in https://github.com/apache/opendal/pull/3260
* ci: Only fetch origin while in pull request by @Xuanwo in https://github.com/apache/opendal/pull/3268
* ci: add a new test case for the disk is full by @sunheyi6 in https://github.com/apache/opendal/pull/3079
* ci: Passing GITHUB_TOKEN to avoid rate limit by @Xuanwo in https://github.com/apache/opendal/pull/3272
* ci(services/hdfs): Use dlcdn.apache.org instead by @Xuanwo in https://github.com/apache/opendal/pull/3308
* ci: Fix HDFS test by @Xuanwo in https://github.com/apache/opendal/pull/3320
* ci: Fix plan not generated correctly for PR from forked repo by @Xuanwo in https://github.com/apache/opendal/pull/3327
* ci(services/azfile): add azfile integration test by @dqhl76 in https://github.com/apache/opendal/pull/3409
* ci: Fix behavior tests been ignored by @Xuanwo in https://github.com/apache/opendal/pull/3422
* ci(binding/java): remove `testWriteFileWithNonAsciiName` behavior test by @G-XD in https://github.com/apache/opendal/pull/3424
* ci(bindings/python): Remove not passing test cases until we addressed by @Xuanwo in https://github.com/apache/opendal/pull/3432
* ci(services/sftp): Move setup logic into docker-compose by @Xuanwo in https://github.com/apache/opendal/pull/3430
* ci(test): Add health check for WebDAV docker compose config by @Zheaoli in https://github.com/apache/opendal/pull/3448
* ci: Switch to 1password connect to avoid rate limit by @Xuanwo in https://github.com/apache/opendal/pull/3447
* ci: Use cargo test instead of carge nextest by @Xuanwo in https://github.com/apache/opendal/pull/3505
* build(bindings/java): Allow building on `linux-aarch_64` by @amunra in https://github.com/apache/opendal/pull/3527
* ci: support behavior test for gridfs by @realtaobo in https://github.com/apache/opendal/pull/3520
### Chore
* chore(ci): publish to pypi with github OIDC credential by @everpcpc in https://github.com/apache/opendal/pull/3252
* chore(bindings/java): align mapping POJO pattern by @tisonkun in https://github.com/apache/opendal/pull/3289
* chore: do not export unreleased bindings by @suyanhanx in https://github.com/apache/opendal/pull/3339
* chore: update object_store unit tests and s3 endpoint docs by @thorseraq in https://github.com/apache/opendal/pull/3345
* chore: Fix typo in mysql doc by @lewiszlw in https://github.com/apache/opendal/pull/3351
* chore: try format yaml files by @suyanhanx in https://github.com/apache/opendal/pull/3364
* chore(bindings/java): move out convert fns by @tisonkun in https://github.com/apache/opendal/pull/3389
* chore(bindings/java): use JDK 8 time APIs by @tisonkun in https://github.com/apache/opendal/pull/3400
* chore: remove unused dependencies by @xxchan in https://github.com/apache/opendal/pull/3414
* chore(test): Compare with digest instead of whole content by @Xuanwo in https://github.com/apache/opendal/pull/3419
* chore: remove useless workflow file by @suyanhanx in https://github.com/apache/opendal/pull/3425
* chore(deps): bump minitrace from 0.5.1 to 0.6.1 by @andylokandy in https://github.com/apache/opendal/pull/3449
* chore(deps): bump korandoru/hawkeye from 3.4.0 to 3.6.0 by @dependabot in https://github.com/apache/opendal/pull/3446
* chore(deps): bump toml from 0.7.8 to 0.8.6 by @dependabot in https://github.com/apache/opendal/pull/3442
* chore(deps): bump actions/setup-node from 3 to 4 by @dependabot in https://github.com/apache/opendal/pull/3445
* chore(deps): bump etcd-client from 0.11.1 to 0.12.1 by @dependabot in https://github.com/apache/opendal/pull/3441
* chore(services/libsql): Fix typos in backend by @sd44 in https://github.com/apache/opendal/pull/3506
* chore: Bump to v0.42.0 to start release process by @silver-ymz in https://github.com/apache/opendal/pull/3509
* chore(service/vercel_artifacts): add doc in backend by @caicancai in https://github.com/apache/opendal/pull/3508
* chore: Remove not released packages while releasing by @Xuanwo in https://github.com/apache/opendal/pull/3519
* chore: Bump to v0.42.0 to start release process (Round 2) by @silver-ymz in https://github.com/apache/opendal/pull/3521
* chore: Fix typo in CHANGELOG  by @caicancai in https://github.com/apache/opendal/pull/3524
* chore: add updated Cargo.toml to git archive by @silver-ymz in https://github.com/apache/opendal/pull/3525
* chore(bindings/java): improve build.py script by @tisonkun in https://github.com/apache/opendal/pull/3529

## [v0.41.0] - 2023-10-08

### Added
* feat: allow using `prometheus-client` crate with PrometheusClientLayer by @flaneur2020 in https://github.com/apache/opendal/pull/3134
* feat(binding/java): support info ops by @G-XD in https://github.com/apache/opendal/pull/3154
* test(binding/java): add behavior test framework by @G-XD in https://github.com/apache/opendal/pull/3129
* feat: Include starting offset for GHAC upload Content-Range by @huonw in https://github.com/apache/opendal/pull/3163
* feat(bindings/cpp): make ReaderStream manage the lifetime of Reader by @silver-ymz in https://github.com/apache/opendal/pull/3164
* feat: Enable multi write for ghac by @Xuanwo in https://github.com/apache/opendal/pull/3165
* feat: Add mysql support for OpenDAL by @Zheaoli in https://github.com/apache/opendal/pull/3170
* feat(service/postgresql): support connection pool by @Zheaoli in https://github.com/apache/opendal/pull/3176
* feat(services/ghac): Allow explicitly setting ghac endpoint/token, not just env vars by @huonw in https://github.com/apache/opendal/pull/3177
* feat(service/azdls): add append support for azdls by @dqhl76 in https://github.com/apache/opendal/pull/3186
* feat(bindings/python): Enable `BlockingLayer` for non-blocking services that don't support blocking by @messense in https://github.com/apache/opendal/pull/3198
* feat: Add write_can_empty in Capability and related tests by @Xuanwo in https://github.com/apache/opendal/pull/3200
* feat: Add basic support for bindings/go using CGO by @jiaoew1991 in https://github.com/apache/opendal/pull/3204
* feat(binding/java): add `copy` test by @G-XD in https://github.com/apache/opendal/pull/3207
* feat(service/sqlite): Support sqlite for opendal by @Zheaoli in https://github.com/apache/opendal/pull/3212
* feat(services/sqlite): Support blocking_get/set/delete in sqlite service by @Zheaoli in https://github.com/apache/opendal/pull/3218
* feat(oay): port `WebdavFs` to dav-server-fs-opendal by @Young-Flash in https://github.com/apache/opendal/pull/3119
### Changed
* refactor(services/dropbox): Use OpWrite instead of passing all args as parameters by @ImSingee in https://github.com/apache/opendal/pull/3126
* refactor(binding/java): read should return bytes by @tisonkun in https://github.com/apache/opendal/pull/3153
* refactor(bindings/java)!: operator jni calls by @tisonkun in https://github.com/apache/opendal/pull/3166
* refactor(tests): reuse function to remove duplicate code by @zhao-gang in https://github.com/apache/opendal/pull/3219
### Fixed
* fix(tests): Create test files one by one instead of concurrently by @Xuanwo in https://github.com/apache/opendal/pull/3132
* chore(ci): fix web identity token path for aws s3 assume role test by @everpcpc in https://github.com/apache/opendal/pull/3141
* fix(services/s3): Detect region returned too early when header is empty by @Xuanwo in https://github.com/apache/opendal/pull/3187
* fix: making OpenDAL compilable on 32hf platforms by @ClSlaid in https://github.com/apache/opendal/pull/3188
* fix(binding/java): decode Java’s modified UTF-8 format by @G-XD in https://github.com/apache/opendal/pull/3195
### Docs
* docs(release): describe how to close the Nexus staging repo by @tisonkun in https://github.com/apache/opendal/pull/3125
* docs: update release docs for cpp and haskell bindings by @silver-ymz in https://github.com/apache/opendal/pull/3130
* docs: Polish VISION to make it more clear by @Xuanwo in https://github.com/apache/opendal/pull/3135
* docs: Add start tracking issues about the next release by @Xuanwo in https://github.com/apache/opendal/pull/3145
* docs: Add download link for 0.40.0 by @Xuanwo in https://github.com/apache/opendal/pull/3149
* docs(bindings/cpp): add more using details about cmake by @silver-ymz in https://github.com/apache/opendal/pull/3155
* docs(bindings/java): Added an example of adding dependencies using Gradle by @eastack in https://github.com/apache/opendal/pull/3158
* docs: include disclaimer in announcement template by @Venderbad in https://github.com/apache/opendal/pull/3172
* docs: Add pants as a user by @huonw in https://github.com/apache/opendal/pull/3180
* docs: Add basic readme for go binding by @Xuanwo in https://github.com/apache/opendal/pull/3206
* docs: add multilingual getting started  by @tisonkun in https://github.com/apache/opendal/pull/3214
* docs: multiple improvements by @tisonkun in https://github.com/apache/opendal/pull/3215
* docs: Add verify script by @Xuanwo in https://github.com/apache/opendal/pull/3239
### CI
* ci: Align tags with semver specs by @Xuanwo in https://github.com/apache/opendal/pull/3136
* ci: Migrate obs to databend labs sponsored bucket by @Xuanwo in https://github.com/apache/opendal/pull/3137
* build(bindings/java): support develop with JDK 21 by @tisonkun in https://github.com/apache/opendal/pull/3140
* ci: Migrate GCS to Databend Labs sponsored bucket by @Xuanwo in https://github.com/apache/opendal/pull/3142
* build(bindings/java): upgrade maven wrapper version by @tisonkun in https://github.com/apache/opendal/pull/3167
* build(bindings/java): support explicit cargo build target by @tisonkun in https://github.com/apache/opendal/pull/3168
* ci: Pin Kvrocks docker image to 2.5.1 to avoid test failure by @git-hulk in https://github.com/apache/opendal/pull/3192
* ci(bindings/ocaml): add doc by @Ranxy in https://github.com/apache/opendal/pull/3208
* build(deps): bump actions/checkout from 3 to 4 by @dependabot in https://github.com/apache/opendal/pull/3222
* build(deps): bump korandoru/hawkeye from 3.3.0 to 3.4.0 by @dependabot in https://github.com/apache/opendal/pull/3223
* build(deps): bump rusqlite from 0.25.4 to 0.29.0 by @dependabot in https://github.com/apache/opendal/pull/3226
### Chore
* chore(bindings/haskell): add rpath to haskell linker option by @silver-ymz in https://github.com/apache/opendal/pull/3128
* chore(ci): add test for aws s3 assume role by @everpcpc in https://github.com/apache/opendal/pull/3139
* chore: Incorrect debug information by @OmAximani0 in https://github.com/apache/opendal/pull/3183
* chore: bump quick-xml version to 0.30 by @Venderbad in https://github.com/apache/opendal/pull/3190
* chore: Let's welcome the contributors from hacktoberfest! by @Xuanwo in https://github.com/apache/opendal/pull/3193
* chore(bindings/java): simplify library path resolution by @tisonkun in https://github.com/apache/opendal/pull/3196
* chore: Make clippy happy by @Xuanwo in https://github.com/apache/opendal/pull/3229


## [v0.40.0] - 2023-09-18

### Added
* feat(service/etcd): support list by @G-XD in https://github.com/apache/opendal/pull/2755
* feat: setup the integrate with PHP binding by @godruoyi in https://github.com/apache/opendal/pull/2726
* feat(oay): Add `read_dir` by @Young-Flash in https://github.com/apache/opendal/pull/2736
* feat(obs): support loading credential from env by @everpcpc in https://github.com/apache/opendal/pull/2767
* feat: add async backtrace layer by @dqhl76 in https://github.com/apache/opendal/pull/2765
* feat: Add OCaml Binding by @Ranxy in https://github.com/apache/opendal/pull/2757
* feat(bindings/haskell): support logging layer by @silver-ymz in https://github.com/apache/opendal/pull/2705
* feat: Add FoundationDB Support for OpenDAL by @ArmandoZ in https://github.com/apache/opendal/pull/2751
* feat(oay): add write for oay webdav by @Young-Flash in https://github.com/apache/opendal/pull/2769
* feat: Implement RFC-2774 Lister API by @Xuanwo in https://github.com/apache/opendal/pull/2787
* feat(bindings/haskell): enhance original `OpMonad` to support custom IO monad by @silver-ymz in https://github.com/apache/opendal/pull/2789
* feat: Add into_seekable_read_by_range support for blocking read by @Xuanwo in https://github.com/apache/opendal/pull/2799
* feat(layers/blocking): add blocking layer by @yah01 in https://github.com/apache/opendal/pull/2780
* feat: Add async list with metakey support by @Xuanwo in https://github.com/apache/opendal/pull/2803
* feat(binding/php): Add basic io by @godruoyi in https://github.com/apache/opendal/pull/2782
* feat: fuzz test support read from .env by different services by @dqhl76 in https://github.com/apache/opendal/pull/2824
* feat(services/rocksdb): Add scan support by @JLerxky in https://github.com/apache/opendal/pull/2827
* feat: Add postgresql support for OpenDAL by @Xuanwo in https://github.com/apache/opendal/pull/2815
* feat: ci for php binding by @godruoyi in https://github.com/apache/opendal/pull/2830
* feat: Add create_dir, remove, copy and rename API for oay-webdav by @Young-Flash in https://github.com/apache/opendal/pull/2832
* feat(oli): oli stat should show path as specified by users by @sarutak in https://github.com/apache/opendal/pull/2842
* feat(services/moka, services/mini-moka): Add scan support by @JLerxky in https://github.com/apache/opendal/pull/2850
* feat(oay): impl some method for `WebdavMetaData` by @Young-Flash in https://github.com/apache/opendal/pull/2857
* feat: Implement list with metakey for blocking by @Xuanwo in https://github.com/apache/opendal/pull/2861
* feat(services/redis): add redis cluster support by @G-XD in https://github.com/apache/opendal/pull/2858
* feat(services/dropbox): read support range by @suyanhanx in https://github.com/apache/opendal/pull/2848
* feat(layers/logging): Allow users to control print backtrace or not by @Xuanwo in https://github.com/apache/opendal/pull/2872
* feat: add native & full capability by @yah01 in https://github.com/apache/opendal/pull/2874
* feat: Implement RFC-2758 Merge Append Into Write by @Xuanwo in https://github.com/apache/opendal/pull/2880
* feat(binding/ocaml): Add support for operator reader and metadata by @Ranxy in https://github.com/apache/opendal/pull/2881
* feat(core): replace field `_pin` with `!Unpin` as argument by @morristai in https://github.com/apache/opendal/pull/2886
* feat: Add retry for Writer::sink operation by @Xuanwo in https://github.com/apache/opendal/pull/2896
* feat: remove operator range_read and range_reader API by @oowl in https://github.com/apache/opendal/pull/2898
* feat(core): Add unit test for ChunkedCursor by @Xuanwo in https://github.com/apache/opendal/pull/2907
* feat(types): remove blocking operation range_read and range_reader API  by @oowl in https://github.com/apache/opendal/pull/2912
* feat(types): add stat_with API for blocking operator by @oowl in https://github.com/apache/opendal/pull/2915
* feat(services/gdrive): credential manage by @suyanhanx in https://github.com/apache/opendal/pull/2914
* feat(core): Implement Exact Buf Writer by @Xuanwo in https://github.com/apache/opendal/pull/2917
* feat: Add benchmark for buf write by @Xuanwo in https://github.com/apache/opendal/pull/2922
* feat(core/raw): Add stream support for multipart by @Xuanwo in https://github.com/apache/opendal/pull/2923
* feat(types): synchronous blocking operator and operator's API by @oowl in https://github.com/apache/opendal/pull/2924
* feat(bindings/java): bundled services by @tisonkun in https://github.com/apache/opendal/pull/2934
* feat(core/raw): support stream body for mixedpart by @silver-ymz in https://github.com/apache/opendal/pull/2936
* feat(bindings/python): expose presign api by @silver-ymz in https://github.com/apache/opendal/pull/2950
* feat(bindings/nodejs): Implement presign test by @suyanhanx in https://github.com/apache/opendal/pull/2969
* docs(services/gdrive): update service doc by @suyanhanx in https://github.com/apache/opendal/pull/2973
* feat(bindings/cpp): init cpp binding by @silver-ymz in https://github.com/apache/opendal/pull/2980
* feat: gcs insert object support cache control by @fatelei in https://github.com/apache/opendal/pull/2974
* feat(bindings/cpp): expose all api returned by value by @silver-ymz in https://github.com/apache/opendal/pull/3001
* feat(services/gdrive): implement rename by @suyanhanx in https://github.com/apache/opendal/pull/3007
* feat(bindings/cpp): expose reader  by @silver-ymz in https://github.com/apache/opendal/pull/3004
* feat(bindings/cpp): expose lister by @silver-ymz in https://github.com/apache/opendal/pull/3011
* feat(core): Avoid copy if input is larger than buffer_size by @Xuanwo in https://github.com/apache/opendal/pull/3016
* feat(service/gdrive): add gdrive list support by @Young-Flash in https://github.com/apache/opendal/pull/3025
* feat(services/etcd): Enable etcd connection pool by @Xuanwo in https://github.com/apache/opendal/pull/3041
* feat: Add buffer support for all services by @Xuanwo in https://github.com/apache/opendal/pull/3045
* feat(bindings/java): auto enable blocking layer by @tisonkun in https://github.com/apache/opendal/pull/3049
* feat(bindings/java): support presign ops by @tisonkun in https://github.com/apache/opendal/pull/3069
* feat(services/azblob): Rewrite the method signatures using OpWrite by @acehinnnqru in https://github.com/apache/opendal/pull/3068
* feat(services/cos): Rewrite the method signatures using OpWrite by @acehinnnqru in https://github.com/apache/opendal/pull/3070
* feat(services/obs): Rewrite method signatures using OpWrite by @hanxuanliang in https://github.com/apache/opendal/pull/3075
* feat(services/cos): Rewrite the methods signature using OpStat/OpRead by @acehinnnqru in https://github.com/apache/opendal/pull/3073
* feat: Add AtomicServer Support for OpenDAL by @ArmandoZ in https://github.com/apache/opendal/pull/2878
* feat(services/onedrive): Rewrite the method signatures using OpWrite by @acehinnnqru in https://github.com/apache/opendal/pull/3091
* feat(services/azblob): Rewrite azblob methods signature using OpRead/OpStat by @acehinnnqru in https://github.com/apache/opendal/pull/3072
* feat(services/obs): Rewrite methods signature in obs using OpRead/OpStat by @hanxuanliang in https://github.com/apache/opendal/pull/3094
* feat(service/gdrive): add gdrive copy by @Young-Flash in https://github.com/apache/opendal/pull/3098
* feat(services/wasabi): Rewrite the method signatures using OpRead,OpW… by @acehinnnqru in https://github.com/apache/opendal/pull/3099
### Changed
* refactor(bindings/haskell): unify ffi of creating operator by @silver-ymz in https://github.com/apache/opendal/pull/2778
* refactor: Remove optimize in into_seekable_read_by_range by @Xuanwo in https://github.com/apache/opendal/pull/2796
* refactor(bindings/ocaml): Refactor module to support documentation by @Ranxy in https://github.com/apache/opendal/pull/2794
* refactor: Implement backtrace for Error correctly by @Xuanwo in https://github.com/apache/opendal/pull/2871
* refactor: Move object_store_opendal to integrations by @Xuanwo in https://github.com/apache/opendal/pull/2888
* refactor(services/gdrive): prepare for CI by @suyanhanx in https://github.com/apache/opendal/pull/2892
* refactor(core): Split buffer logic from underlying storage operations by @Xuanwo in https://github.com/apache/opendal/pull/2903
* refactor(service/webdav): Add docker-compose file to simplify the CI by @dqhl76 in https://github.com/apache/opendal/pull/2873
* refactor(raw): Return written bytes in oio::Write by @Xuanwo in https://github.com/apache/opendal/pull/3005
* refactor: Refactor oio::Write by accepting oio::Reader instead by @Xuanwo in https://github.com/apache/opendal/pull/3008
* refactor(core): Rename confusing pipe into copy_from by @Xuanwo in https://github.com/apache/opendal/pull/3015
* refactor: Remove oio::Write::copy_from by @Xuanwo in https://github.com/apache/opendal/pull/3018
* refactor: Make oio::Write accept Buf instead by @Xuanwo in https://github.com/apache/opendal/pull/3021
* refactor: Relax bounds on Writer::{sink, copy} by @huonw in https://github.com/apache/opendal/pull/3027
* refactor: Refactor oio::Write into poll-based to create more room for optimization by @Xuanwo in https://github.com/apache/opendal/pull/3029
* refactor: Polish multipart writer to allow oneshot optimization by @Xuanwo in https://github.com/apache/opendal/pull/3031
* refactor: Polish implementation details of WriteBuf and add vector chunks support by @Xuanwo in https://github.com/apache/opendal/pull/3034
* refactor: Add ChunkedBytes to improve the exact buf write by @Xuanwo in https://github.com/apache/opendal/pull/3035
* refactor: Polish RangeWrite implementation to remove the extra buffer logic by @Xuanwo in https://github.com/apache/opendal/pull/3038
* refactor: Remove the requirement of passing `content_length` to writer by @Xuanwo in https://github.com/apache/opendal/pull/3044
* refactor(services/azblob): instead `parse_batch_delete_response` with `Multipart::parse` by @G-XD in https://github.com/apache/opendal/pull/3071
* refactor(services/webdav): Refactor `webdav_put` signatures by using `OpWrite`. by @laipz8200 in https://github.com/apache/opendal/pull/3076
* refactor(services/azdls): Use OpWrite instead of passing all args as parameters by @liul85 in https://github.com/apache/opendal/pull/3077
* refactor(services/webdav): Use OpRead in `webdav_get`. by @laipz8200 in https://github.com/apache/opendal/pull/3081
* refactor(services/oss): Refactor `oss_put_object` signatures by using OpWrite by @sysu-yunz in https://github.com/apache/opendal/pull/3080
* refactor(services/http): Rewrite `http` methods signature by using OpRead/OpStat by @miroim in https://github.com/apache/opendal/pull/3083
* refactor(services/gcs): Rewrite `gcs` methods signature by using OpXxxx by @wavty in https://github.com/apache/opendal/pull/3087
* refactor: move all `fixtures` from `core/src/services/{service}` to top-level `fixtures/{service}` by @G-XD in https://github.com/apache/opendal/pull/3088
* refactor(services/webhdfs): Rewrite `webhdfs` methods signature by using `OpXxxx` by @cxorm in https://github.com/apache/opendal/pull/3109
### Fixed
* fix(docs): KEYS broken link by @suyanhanx in https://github.com/apache/opendal/pull/2749
* fix: scheme from_str missing redb and tikv by @Ranxy in https://github.com/apache/opendal/pull/2766
* fix(ci): pin zig version to 0.11.0 by @oowl in https://github.com/apache/opendal/pull/2772
* fix: fix compile error by low version of backon in old project by @silver-ymz in https://github.com/apache/opendal/pull/2781
* fix: Bump openssh-sftp-client from 0.13.5 to 0.13.7 by @yah01 in https://github.com/apache/opendal/pull/2797
* fix: add redis for nextcloud to solve file locking problem by @dqhl76 in https://github.com/apache/opendal/pull/2805
* fix: Fix behavior tests for blocking layer by @Xuanwo in https://github.com/apache/opendal/pull/2809
* fix(services/s3): remove default region `us-east-1` for non-aws s3 by @G-XD in https://github.com/apache/opendal/pull/2812
* fix(oli): Fix a test name in ls.rs by @sarutak in https://github.com/apache/opendal/pull/2817
* fix(oli, doc): Fix examples of config.toml for oli by @sarutak in https://github.com/apache/opendal/pull/2819
* fix: Cleanup temporary files generated in tests automatically by @sarutak in https://github.com/apache/opendal/pull/2823
* fix(services/rocksdb): Make sure return key starts with input path by @Xuanwo in https://github.com/apache/opendal/pull/2828
* fix(services/sftp): bump openssh-sftp-client to 0.13.9 by @silver-ymz in https://github.com/apache/opendal/pull/2831
* fix(oli): oli commands don't work properly for files in CWD by @sarutak in https://github.com/apache/opendal/pull/2833
* fix(oli): oli commands should not accept invalid URI format by @sarutak in https://github.com/apache/opendal/pull/2845
* fix(bindings/c): Fix an example of the C binding by @sarutak in https://github.com/apache/opendal/pull/2854
* fix(doc): Update instructions for building the C binding in README.md by @sarutak in https://github.com/apache/opendal/pull/2856
* fix(oay): add some error handle by @Young-Flash in https://github.com/apache/opendal/pull/2879
* fix: Set default timeouts for HttpClient by @sarutak in https://github.com/apache/opendal/pull/2895
* fix(website): broken edit link by @suyanhanx in https://github.com/apache/opendal/pull/2913
* fix(binding/java): Overwrite default NOTICE file with correct years by @tisonkun in https://github.com/apache/opendal/pull/2918
* fix(services/gcs): migrate to new multipart impl for gcs_insert_object_request by @silver-ymz in https://github.com/apache/opendal/pull/2838
* fix(core): Invalid lister should not panic nor endless loop by @Xuanwo in https://github.com/apache/opendal/pull/2931
* fix: Enable exact_buf_write for R2 by @Xuanwo in https://github.com/apache/opendal/pull/2935
* fix(services/s3): allow 404 resp when deleting a non-existing object by @gongyisheng in https://github.com/apache/opendal/pull/2941
* fix(doc): use crate::docs::rfc to replace relative path in doc by @gongyisheng in https://github.com/apache/opendal/pull/2942
* fix: S3 copy error on non-ascii file path by @BoWuGit in https://github.com/apache/opendal/pull/2909
* fix: copy error on non-ascii file path for cos/obs/wasabi services by @BoWuGit in https://github.com/apache/opendal/pull/2948
* fix(doc): add GCS api reference and known issues to service/s3 doc by @gongyisheng in https://github.com/apache/opendal/pull/2949
* fix(oay): pass litmus copymove test by @Young-Flash in https://github.com/apache/opendal/pull/2944
* fix(core): Make sure OpenDAL works with http2 on GCS by @Xuanwo in https://github.com/apache/opendal/pull/2956
* fix(nodejs|java): Add place holder for BDD test by @Xuanwo in https://github.com/apache/opendal/pull/2962
* fix(core): Fix capability of services is not set correctly by @Xuanwo in https://github.com/apache/opendal/pull/2968
* fix(core): Fix capability of services is not set correctly by @JLerxky in https://github.com/apache/opendal/pull/2982
* fix(services/gcs): Fix handling of media and multipart insert by @Xuanwo in https://github.com/apache/opendal/pull/2997
* fix(services/webdav): decode path before set Entry by @G-XD in https://github.com/apache/opendal/pull/3020
* fix(services/oss): set content_md5 in lister by @G-XD in https://github.com/apache/opendal/pull/3043
* fix: Correct the name of azdfs to azdls by @Xuanwo in https://github.com/apache/opendal/pull/3046
* fix: Don't apply blocking layer when service support blocking by @Xuanwo in https://github.com/apache/opendal/pull/3050
* fix: call `flush` before `sync_all` by @WenyXu in https://github.com/apache/opendal/pull/3053
* fix: Metakeys are not propagated with the blocking operators by @Xuanwo in https://github.com/apache/opendal/pull/3116
### Docs
* doc: fix released doc minor error by @oowl in https://github.com/apache/opendal/pull/2737
* docs: create README.md for oli by @STRRL in https://github.com/apache/opendal/pull/2752
* docs: polish fuzz README by @dqhl76 in https://github.com/apache/opendal/pull/2777
* docs: Add an example for PostgreSQL service by @sarutak in https://github.com/apache/opendal/pull/2847
* docs: improve php binding documentation by @godruoyi in https://github.com/apache/opendal/pull/2843
* docs: Fix missing link for rust example by @sarutak in https://github.com/apache/opendal/pull/2866
* docs: Add blog on how opendal read data by @Xuanwo in https://github.com/apache/opendal/pull/2869
* docs: Fix missing link to the contribution guide for the Node.js binding by @sarutak in https://github.com/apache/opendal/pull/2876
* doc: add 0.39.0 release link to download.md by @oowl in https://github.com/apache/opendal/pull/2882
* doc: add missing release step by @oowl in https://github.com/apache/opendal/pull/2883
* docs: add new committer landing doc  by @dqhl76 in https://github.com/apache/opendal/pull/2905
* docs: auto release maven artifacts by @tisonkun in https://github.com/apache/opendal/pull/2729
* doc(tests): fix test command by @G-XD in https://github.com/apache/opendal/pull/2920
* docs: add service doc for gcs by @silver-ymz in https://github.com/apache/opendal/pull/2930
* docs(services/gcs): fix rust core doc include by @suyanhanx in https://github.com/apache/opendal/pull/2932
* docs: migrate all existed service documents by @silver-ymz in https://github.com/apache/opendal/pull/2937
* docs: Fix incorrect links to rfcs by @Xuanwo in https://github.com/apache/opendal/pull/2943
* docs: Update Release Process by @Xuanwo in https://github.com/apache/opendal/pull/2964
* docs(services/sftp): update comments about windows support and password login support by @silver-ymz in https://github.com/apache/opendal/pull/2967
* docs: add service doc for etcd & dropbox & foundationdb & moka by @G-XD in https://github.com/apache/opendal/pull/2986
* docs(bindings/cpp): add CONTRIBUTING.md  by @silver-ymz in https://github.com/apache/opendal/pull/2984
* docs(bindings/cpp): use doxygen to generate API docs by @silver-ymz in https://github.com/apache/opendal/pull/2988
* docs(bindings/c): add awesome-doxygen to beautify document by @silver-ymz in https://github.com/apache/opendal/pull/2999
* docs(contributing): add podling status report guide by @PsiACE in https://github.com/apache/opendal/pull/2996
* docs: fix spelling - change `Github` to `GitHub` by @jbampton in https://github.com/apache/opendal/pull/3012
* docs: fix spelling - change `MacOS` to `macOS` by @jbampton in https://github.com/apache/opendal/pull/3013
* docs: add service doc for gdrive & onedrive by @nasnoisaac in https://github.com/apache/opendal/pull/3028
* docs(services/sftp): update comments about password login by @silver-ymz in https://github.com/apache/opendal/pull/3065
* docs: Add OwO 1st by @Xuanwo in https://github.com/apache/opendal/pull/3086
* docs: Add upgrade note for v0.40 by @Xuanwo in https://github.com/apache/opendal/pull/3096
* docs: add basic example for cpp binding by @silver-ymz in https://github.com/apache/opendal/pull/3108
* docs: Add comments for blocking layer by @Xuanwo in https://github.com/apache/opendal/pull/3117
### CI
* build(deps): bump serde_json from 1.0.99 to 1.0.104 by @dependabot in https://github.com/apache/opendal/pull/2746
* build(deps): bump tracing-opentelemetry from 0.17.4 to 0.19.0 by @dependabot in https://github.com/apache/opendal/pull/2744
* build(deps): bump paste from 1.0.13 to 1.0.14 by @dependabot in https://github.com/apache/opendal/pull/2742
* build(deps): bump opentelemetry from 0.19.0 to 0.20.0 by @dependabot in https://github.com/apache/opendal/pull/2743
* build(deps): bump object_store from 0.5.6 to 0.6.1 by @dependabot in https://github.com/apache/opendal/pull/2745
* ci: use cache to speed up haskell ci by @silver-ymz in https://github.com/apache/opendal/pull/2792
* ci: Add setup for php and ocaml in dev container by @Xuanwo in https://github.com/apache/opendal/pull/2825
* ci: Trying to fix rocksdb build by @Xuanwo in https://github.com/apache/opendal/pull/2867
* ci: add reproducibility check by @tisonkun in https://github.com/apache/opendal/pull/2863
* ci(services/postgresql): add docker-compose to simplify the CI by @G-XD in https://github.com/apache/opendal/pull/2877
* ci(service/s3): Add docker-compose-minio file to simplify the CI by @gongyisheng in https://github.com/apache/opendal/pull/2887
* ci(services/hdfs): Load native lib instead by @Xuanwo in https://github.com/apache/opendal/pull/2900
* ci(services/rocksdb): Make sure rocksdb lib is loaded by @Xuanwo in https://github.com/apache/opendal/pull/2902
* build(bindings/java): bundle bare binaries in JARs with classifier by @tisonkun in https://github.com/apache/opendal/pull/2910
* ci(bindings/java): enable auto staging JARs on Apache Nexus repository by @tisonkun in https://github.com/apache/opendal/pull/2939
* ci(fix): Add PORTABLE to make sure rocksdb compiled with the same CPU feature set by @gongyisheng in https://github.com/apache/opendal/pull/2976
* ci(oay): Polish oay webdav test by @Young-Flash in https://github.com/apache/opendal/pull/2971
* build(deps): bump cbindgen from 0.24.5 to 0.25.0 by @dependabot in https://github.com/apache/opendal/pull/2992
* build(deps): bump actions/checkout from 2 to 3 by @dependabot in https://github.com/apache/opendal/pull/2995
* build(deps): bump pin-project from 1.1.2 to 1.1.3 by @dependabot in https://github.com/apache/opendal/pull/2993
* build(deps): bump chrono from 0.4.26 to 0.4.28 by @dependabot in https://github.com/apache/opendal/pull/2989
* build(deps): bump redb from 1.0.4 to 1.1.0 by @dependabot in https://github.com/apache/opendal/pull/2991
* build(deps): bump lazy-regex from 2.5.0 to 3.0.1 by @dependabot in https://github.com/apache/opendal/pull/2990
* build(deps): bump korandoru/hawkeye from 3.1.0 to 3.3.0 by @dependabot in https://github.com/apache/opendal/pull/2994
* ci(bindings/cpp): add ci for test and doc by @silver-ymz in https://github.com/apache/opendal/pull/2998
* ci(services/tikv): add tikv integration test with tls by @G-XD in https://github.com/apache/opendal/pull/3026
* ci: restrict workflow that need password by @dqhl76 in https://github.com/apache/opendal/pull/3039
* ci: Don't release while tag contains rc by @Xuanwo in https://github.com/apache/opendal/pull/3048
* ci(bindings/java): skip RedisServiceTest on macos and windows by @tisonkun in https://github.com/apache/opendal/pull/3054
* ci: Disable PHP build temporarily by @Xuanwo in https://github.com/apache/opendal/pull/3058
* ci(bindings/java): release workflow always uses bash by @tisonkun in https://github.com/apache/opendal/pull/3056
* ci(binding/java): Enable release build only when releasing by @Xuanwo in https://github.com/apache/opendal/pull/3057
* ci(binding/java): Use cargo profile instead of --release by @Xuanwo in https://github.com/apache/opendal/pull/3059
* ci: Move platform build checks from java binding to rust core by @Xuanwo in https://github.com/apache/opendal/pull/3060
* ci(bindings/haskell): add release workflow by @silver-ymz in https://github.com/apache/opendal/pull/3082
* ci: Build rc but don't publish by @Xuanwo in https://github.com/apache/opendal/pull/3089
* ci: Don't verify content for dry run by @Xuanwo in https://github.com/apache/opendal/pull/3115
### Chore
* chore(core): bump cargo.toml http version to 0.2.9 by @oowl in https://github.com/apache/opendal/pull/2740
* chore: do not export example directory by @oowl in https://github.com/apache/opendal/pull/2750
* chore: Fix build after merging of ocaml by @Xuanwo in https://github.com/apache/opendal/pull/2776
* chore: Bump bytes to 1.4 to allow the usage of spare_capacity_mut by @Xuanwo in https://github.com/apache/opendal/pull/2784
* chore: disable oldtime feature of chrono by @paolobarbolini in https://github.com/apache/opendal/pull/2793
* chore: Disable blocking layer until we make all services passed by @Xuanwo in https://github.com/apache/opendal/pull/2806
* chore(bindings/haskell): post release 0.1.0 by @silver-ymz in https://github.com/apache/opendal/pull/2814
* chore(bindings/ocaml): Add contributing document to readme by @Ranxy in https://github.com/apache/opendal/pull/2829
* chore: Make clippy happy by @Xuanwo in https://github.com/apache/opendal/pull/2851
* chore: add health check for docker-compose minio by @oowl in https://github.com/apache/opendal/pull/2899
* chore(ci): offload healthcheck logic to docker-compose config by @oowl in https://github.com/apache/opendal/pull/2901
* chore: Make clippy happy by @Xuanwo in https://github.com/apache/opendal/pull/2927
* chore: Make C Binding clippy happy by @Xuanwo in https://github.com/apache/opendal/pull/2928
* chore: Fix failed ci by @silver-ymz in https://github.com/apache/opendal/pull/2938
* chore(ci): remove unreviewable test file and add generate test file step before testing  by @gongyisheng in https://github.com/apache/opendal/pull/3003
* chore(bindings/cpp): update CMakeLists.txt to prepare release by @silver-ymz in https://github.com/apache/opendal/pull/3030
* chore: fix typo of SftpWriter error message by @silver-ymz in https://github.com/apache/opendal/pull/3032
* chore: Polish some details of layers implementation by @Xuanwo in https://github.com/apache/opendal/pull/3061
* chore(bindings/haskell): make cargo build type same with cabal by @silver-ymz in https://github.com/apache/opendal/pull/3067
* chore(bindings/haskell): add PVP-compliant version bounds by @silver-ymz in https://github.com/apache/opendal/pull/3093
* chore(bindings/java): align ErrorKind with exception code by @tisonkun in https://github.com/apache/opendal/pull/3095
* chore: Bump version to v0.40 to start release process by @Xuanwo in https://github.com/apache/opendal/pull/3101
* chore(bindings/haskell): rename library name from opendal-hs to opendal by @silver-ymz in https://github.com/apache/opendal/pull/3112

## [v0.39.0] - 2023-07-31

### Added
* feat: add a behaviour test for InvalidInput by @dqhl76 in https://github.com/apache/opendal/pull/2644
* feat(services/persy): add a basic persy service impl by @PsiACE in https://github.com/apache/opendal/pull/2648
* feat(services/vercel_artifacts): Impl `stat` by @suyanhanx in https://github.com/apache/opendal/pull/2649
* feat(test): add fuzz test for range_reader by @dqhl76 in https://github.com/apache/opendal/pull/2609
* feat(core/http_util): Remove sensitive header like Set-Cookie by @Xuanwo in https://github.com/apache/opendal/pull/2664
* feat: Add RetryInterceptor support for RetryLayer by @Xuanwo in https://github.com/apache/opendal/pull/2666
* feat: support kerberos for hdfs service by @zuston in https://github.com/apache/opendal/pull/2668
* feat: support append for hdfs by @zuston in https://github.com/apache/opendal/pull/2671
* feat(s3): Use us-east-1 while head bucket returns 403 without X-Amz-Bucket-Region by @john8628 in https://github.com/apache/opendal/pull/2677
* feat(oay): Add webdav basic read impl by @Young-Flash in https://github.com/apache/opendal/pull/2658
* feat(services/redis): enable TLS by @Stormshield-robinc in https://github.com/apache/opendal/pull/2670
* feat(services/etcd): introduce new service backend etcd by @G-XD in https://github.com/apache/opendal/pull/2672
* feat(service/obs):add multipart upload function support by @A-Stupid-Sun in https://github.com/apache/opendal/pull/2685
* feat(services/s3): Add assume role support by @Xuanwo in https://github.com/apache/opendal/pull/2687
* feat(services/tikv): introduce new service backend tikv by @oowl in https://github.com/apache/opendal/pull/2565
* feat(service/cos): add multipart upload function support by @ArmandoZ in https://github.com/apache/opendal/pull/2697
* feat(oio): Add MultipartUploadWrite to easier the work for Writer by @Xuanwo in https://github.com/apache/opendal/pull/2699
* feat(test): add fuzz target for writer by @dqhl76 in https://github.com/apache/opendal/pull/2706
* feat: cos multipart uploads write by @parkma99 in https://github.com/apache/opendal/pull/2712
* feat(layers): support await_tree instrument by @oowl in https://github.com/apache/opendal/pull/2623
* feat(tests): Extract fuzz test of #2717 by @Xuanwo in https://github.com/apache/opendal/pull/2720
* feat: oss multipart uploads write by @parkma99 in https://github.com/apache/opendal/pull/2723
* feat: add override_content_type by @G-XD in https://github.com/apache/opendal/pull/2734
### Changed
* refactor(services/redis): Polish features of redis by @Xuanwo in https://github.com/apache/opendal/pull/2681
* refactor(services/s3): Check header first for region detect by @Xuanwo in https://github.com/apache/opendal/pull/2691
* refactor(raw/oio): Reorganize to allow adding more features by @Xuanwo in https://github.com/apache/opendal/pull/2698
* refactor: Polish fuzz build time by @Xuanwo in https://github.com/apache/opendal/pull/2721
### Fixed
* fix(services/cos): fix cos service comments by @A-Stupid-Sun in https://github.com/apache/opendal/pull/2656
* fix(test): profile setting warning by @dqhl76 in https://github.com/apache/opendal/pull/2657
* fix(bindings/C): fix the memory found in valgrind. by @Ji-Xinyou in https://github.com/apache/opendal/pull/2673
* fix: owncloud test sometimes fail by @dqhl76 in https://github.com/apache/opendal/pull/2684
* fix(services/obs): remove content-length check in backend by @suyanhanx in https://github.com/apache/opendal/pull/2686
* fix: fix `HADOOP_CONF_DIR` setting in guidance document by @wcy-fdu in https://github.com/apache/opendal/pull/2713
* fix: Seek before the start of file should be invalid by @Xuanwo in https://github.com/apache/opendal/pull/2718
* fix(layer/minitrace): fix doctest by @andylokandy in https://github.com/apache/opendal/pull/2728
### Docs
* docs: add instructions to fix wrong vote mail and uploads by @ClSlaid in https://github.com/apache/opendal/pull/2682
* doc(services/tikv): add tikv service backend to readme by @oowl in https://github.com/apache/opendal/pull/2711
* docs(bindings/java): improve safety doc for get_current_env by @tisonkun in https://github.com/apache/opendal/pull/2733
### CI
* ci(services/webdav): Setup integration test for owncloud by @dqhl76 in https://github.com/apache/opendal/pull/2659
* ci: Fix unexpected error in owncloud by @Xuanwo in https://github.com/apache/opendal/pull/2663
* ci: upgrade hawkeye action by @tisonkun in https://github.com/apache/opendal/pull/2665
* ci: Make owncloud happy by reduce the concurrency by @Xuanwo in https://github.com/apache/opendal/pull/2667
* ci: Setup protoc in rust builder by @Xuanwo in https://github.com/apache/opendal/pull/2674
* ci: Fix Cargo.lock not updated by @Xuanwo in https://github.com/apache/opendal/pull/2680
* ci: Add services fuzz test for read/write/range_read by @dqhl76 in https://github.com/apache/opendal/pull/2710
### Chore
* chore: Update CODEOWNERS by @Xuanwo in https://github.com/apache/opendal/pull/2676
* chore(bindings/python): upgrade pyo3 to 0.19 by @messense in https://github.com/apache/opendal/pull/2694
* chore: upgrade quick-xml to 0.29 by @messense in https://github.com/apache/opendal/pull/2696
* chore(download): update version 0.38.1 by @suyanhanx in https://github.com/apache/opendal/pull/2714
* chore(service/minitrace): update to v0.5.0 by @andylokandy in https://github.com/apache/opendal/pull/2725


## [v0.38.1] - 2023-07-14

### Added

- feat(binding/lua): add rename and create_dir operator function by @oowl in https://github.com/apache/opendal/pull/2564
- feat(services/azblob): support sink by @suyanhanx in https://github.com/apache/opendal/pull/2574
- feat(services/gcs): support sink by @suyanhanx in https://github.com/apache/opendal/pull/2576
- feat(services/oss): support sink by @suyanhanx in https://github.com/apache/opendal/pull/2577
- feat(services/obs): support sink by @suyanhanx in https://github.com/apache/opendal/pull/2578
- feat(services/cos): impl sink by @suyanhanx in https://github.com/apache/opendal/pull/2587
- feat(service): Support stat for Dropbox by @Zheaoli in https://github.com/apache/opendal/pull/2588
- feat(services/dropbox): impl create_dir and polish error handling by @suyanhanx in https://github.com/apache/opendal/pull/2600
- feat(services/dropbox): Implement refresh token support by @Xuanwo in https://github.com/apache/opendal/pull/2604
- feat(service/dropbox): impl batch delete by @suyanhanx in https://github.com/apache/opendal/pull/2606
- feat(CI): set Kvrocks test for service redis by @suyanhanx in https://github.com/apache/opendal/pull/2613
- feat(core): object versioning APIs by @suyanhanx in https://github.com/apache/opendal/pull/2614
- feat(oay): actually read configuration from `oay.toml` by @messense in https://github.com/apache/opendal/pull/2615
- feat(services/webdav): impl sink by @suyanhanx in https://github.com/apache/opendal/pull/2622
- feat(services/fs): impl Sink for Fs by @Ji-Xinyou in https://github.com/apache/opendal/pull/2626
- feat(core): impl `delete_with` on blocking operator by @suyanhanx in https://github.com/apache/opendal/pull/2633
- feat(bindings/C): add support for list in C binding by @Ji-Xinyou in https://github.com/apache/opendal/pull/2448
- feat(services/s3): Add detect_region support for S3Builder by @parkma99 in https://github.com/apache/opendal/pull/2634

### Changed

- refactor(core): Add ErrorKind InvalidInput to indicate users input error by @dqhl76 in https://github.com/apache/opendal/pull/2637
- refactor(services/s3): Add more detect logic for detect_region by @Xuanwo in https://github.com/apache/opendal/pull/2645

### Fixed

- fix(doc): fix codeblock rendering by @xxchan in https://github.com/apache/opendal/pull/2592
- fix(service/minitrace): should set local parent by @andylokandy in https://github.com/apache/opendal/pull/2620
- fix(service/minitrace): update doc by @andylokandy in https://github.com/apache/opendal/pull/2621

### Docs

- doc(bindings/haskell): add module document by @silver-ymz in https://github.com/apache/opendal/pull/2566
- docs: Update license related comments by @Prashanth-Chandra in https://github.com/apache/opendal/pull/2573
- docs: add hdfs namenode High Availability related troubleshoot by @wcy-fdu in https://github.com/apache/opendal/pull/2601
- docs: polish release doc by @PsiACE in https://github.com/apache/opendal/pull/2608
- docs(blog): add Apache OpenDAL(Incubating): Access Data Freely by @PsiACE in https://github.com/apache/opendal/pull/2607
- docs(RFC): Object Versioning by @suyanhanx in https://github.com/apache/opendal/pull/2602

### CI

- ci: Disable bindings/java deploy for now by @tisonkun in https://github.com/apache/opendal/pull/2560
- ci: Disable the failed stage-release job instead by @tisonkun in https://github.com/apache/opendal/pull/2561
- ci: add haddock generator for haskell binding by @silver-ymz in https://github.com/apache/opendal/pull/2569
- ci(binding/lua): add luarocks package manager support by @oowl in https://github.com/apache/opendal/pull/2558
- build(deps): bump predicates from 2.1.5 to 3.0.1 by @dependabot in https://github.com/apache/opendal/pull/2583
- build(deps): bump tower-http from 0.4.0 to 0.4.1 by @dependabot in https://github.com/apache/opendal/pull/2582
- build(deps): bump chrono from 0.4.24 to 0.4.26 by @dependabot in https://github.com/apache/opendal/pull/2581
- build(deps): bump redis from 0.22.3 to 0.23.0 by @dependabot in https://github.com/apache/opendal/pull/2580
- build(deps): bump cbindgen from 0.24.3 to 0.24.5 by @dependabot in https://github.com/apache/opendal/pull/2579
- ci: upgrade hawkeye to v3 by @tisonkun in https://github.com/apache/opendal/pull/2585
- ci(services/webdav): Setup integration test for nextcloud by @Xuanwo in https://github.com/apache/opendal/pull/2631

### Chore

- chore: add haskell binding link to website by @silver-ymz in https://github.com/apache/opendal/pull/2571
- chore: fix cargo warning for resolver by @xxchan in https://github.com/apache/opendal/pull/2590
- chore: bump log to 0.4.19 by @xxchan in https://github.com/apache/opendal/pull/2591
- chore(deps): update deps to latest version by @suyanhanx in https://github.com/apache/opendal/pull/2596
- chore: Add release 0.38.0 to download by @PsiACE in https://github.com/apache/opendal/pull/2597
- chore(service/minitrace): automatically generate span name by @andylokandy in https://github.com/apache/opendal/pull/2618

## New Contributors

- @Prashanth-Chandra made their first contribution in https://github.com/apache/opendal/pull/2573
- @andylokandy made their first contribution in https://github.com/apache/opendal/pull/2618
- @parkma99 made their first contribution in https://github.com/apache/opendal/pull/2634

## [v0.38.0] - 2023-06-27

### Added

- feat(raw/http_util): Implement mixed multipart parser by @Xuanwo in https://github.com/apache/opendal/pull/2430
- feat(services/gcs): Add batch delete support by @wcy-fdu in https://github.com/apache/opendal/pull/2142
- feat(core): Add Write::sink API by @Xuanwo in https://github.com/apache/opendal/pull/2440
- feat(services/s3): Allow retry for unexpected 499 error by @Xuanwo in https://github.com/apache/opendal/pull/2453
- feat(layer): add throttle layer by @morristai in https://github.com/apache/opendal/pull/2444
- feat(bindings/haskell): init haskell binding by @silver-ymz in https://github.com/apache/opendal/pull/2463
- feat(core): add capability check by @unixzii in https://github.com/apache/opendal/pull/2461
- feat(bindings/haskell): add CONTRIBUTING.md by @silver-ymz in https://github.com/apache/opendal/pull/2466
- feat(bindings/haskell): add CI test for haskell binding by @silver-ymz in https://github.com/apache/opendal/pull/2468
- feat(binding/lua): introduce opendal lua binding by @oowl in https://github.com/apache/opendal/pull/2469
- feat(bindings/swift): add Swift binding by @unixzii in https://github.com/apache/opendal/pull/2470
- feat(bindings/haskell): support `is_exist` `create_dir` `copy` `rename` `delete` by @silver-ymz in https://github.com/apache/opendal/pull/2475
- feat(bindings/haskell): add `Monad` wrapper by @silver-ymz in https://github.com/apache/opendal/pull/2482
- feat(bindings/dotnet): basic structure by @tisonkun in https://github.com/apache/opendal/pull/2485
- feat(services/dropbox): Support create/read/delete for Dropbox by @Zheaoli in https://github.com/apache/opendal/pull/2264
- feat(bindings/java): support load system lib by @tisonkun in https://github.com/apache/opendal/pull/2502
- feat(blocking operator): add remove_all api by @infdahai in https://github.com/apache/opendal/pull/2449
- feat(core): adopt WebHDFS LISTSTATUS_BATCH for better performance by @morristai in https://github.com/apache/opendal/pull/2499
- feat(bindings/haskell): support stat by @silver-ymz in https://github.com/apache/opendal/pull/2504
- feat(adapters-kv): add rename and copy support to kv adapters by @oowl in https://github.com/apache/opendal/pull/2513
- feat: Implement sink for services s3 by @Xuanwo in https://github.com/apache/opendal/pull/2508
- feat(adapters-kv): add rename and copy support to non typed kv adapters by @oowl in https://github.com/apache/opendal/pull/2515
- feat: Implement test harness via libtest-mimic instead by @Xuanwo in https://github.com/apache/opendal/pull/2517
- feat(service/sled): introduce tree support by @oowl in https://github.com/apache/opendal/pull/2516
- feat(bindings/haskell): support list and scan by @silver-ymz in https://github.com/apache/opendal/pull/2527
- feat(services/redb): support redb service by @oowl in https://github.com/apache/opendal/pull/2526
- feat(core): implement service for Mini Moka by @morristai in https://github.com/apache/opendal/pull/2537
- feat(core): add Mini Moka GitHub Action workflow job by @morristai in https://github.com/apache/opendal/pull/2539
- feat(services): add cacache backend by @PsiACE in https://github.com/apache/opendal/pull/2548
- feat: Implement Writer::copy so user can copy from AsyncRead by @Xuanwo in https://github.com/apache/opendal/pull/2552

### Changed

- refactor(bindings/C): refactor c bindings to call all APIs using pointer by @Ji-Xinyou in https://github.com/apache/opendal/pull/2489

### Fixed

- fix(services/azblob): Fix azblob batch max operations by @A-Stupid-Sun in https://github.com/apache/opendal/pull/2434
- fix(services/sftp): change default root config to remote server setting by @silver-ymz in https://github.com/apache/opendal/pull/2431
- fix: Enable `std` feature for futures to allow `futures::AsyncRead` by @Xuanwo in https://github.com/apache/opendal/pull/2450
- fix(services/gcs): GCS should support create dir by @Xuanwo in https://github.com/apache/opendal/pull/2467
- fix(bindings/C): use copy_from_slice instead of from_static in opendal_bytes by @Ji-Xinyou in https://github.com/apache/opendal/pull/2473
- fix(bindings/swift): reorg the package to correct its name by @unixzii in https://github.com/apache/opendal/pull/2479
- fix: Fix the build for zig binding by @Xuanwo in https://github.com/apache/opendal/pull/2493
- fix(service/webhdfs): fix webhdfs config builder for disable_list_batch by @morristai in https://github.com/apache/opendal/pull/2509
- fix(core/types): add missing `vercel artifacts` for `FromStr` by @cijiugechu in https://github.com/apache/opendal/pull/2519
- fix(types/operator): fix operation limit error default size by @oowl in https://github.com/apache/opendal/pull/2536

### Docs

- docs: Replace `create` with `new` by @NiwakaDev in https://github.com/apache/opendal/pull/2427
- docs(services/redis): fix redis via config example by @A-Stupid-Sun in https://github.com/apache/opendal/pull/2443
- docs: add rust usage example by @Young-Flash in https://github.com/apache/opendal/pull/2447
- docs: Polish rust examples by @Xuanwo in https://github.com/apache/opendal/pull/2456
- docs: polish docs and fix typos by @suyanhanx in https://github.com/apache/opendal/pull/2458
- docs: fix a typo on the landing page by @unixzii in https://github.com/apache/opendal/pull/2460
- docs(examples/rust): Add 01-init-operator by @Xuanwo in https://github.com/apache/opendal/pull/2464
- docs: update readme.md to match the output by @rrain7 in https://github.com/apache/opendal/pull/2486
- docs: Update components for Libraries and Services by @Xuanwo in https://github.com/apache/opendal/pull/2487
- docs: Add OctoBase into our users list by @Xuanwo in https://github.com/apache/opendal/pull/2506
- docs: Fix scan not checked for sled services by @Xuanwo in https://github.com/apache/opendal/pull/2507
- doc(binding/lua): Improve readme doc for contribute and usage by @oowl in https://github.com/apache/opendal/pull/2511
- doc(services/redb): add doc for redb service backend by @oowl in https://github.com/apache/opendal/pull/2538
- doc(bindings/swift): add CONTRIBUTING.md by @unixzii in https://github.com/apache/opendal/pull/2540
- docs: Add new rust example 02-async-io by @Xuanwo in https://github.com/apache/opendal/pull/2541
- docs: Fix link for CONTRIBUTING.md by @HuSharp in https://github.com/apache/opendal/pull/2544
- doc: polish release doc by @suyanhanx in https://github.com/apache/opendal/pull/2531
- docs: Move verify to upper folder by @Xuanwo in https://github.com/apache/opendal/pull/2546
- doc(binding/lua): add ldoc generactor for lua binding by @oowl in https://github.com/apache/opendal/pull/2549
- docs: Add new architectural image for OpenDAL by @Xuanwo in https://github.com/apache/opendal/pull/2553
- docs: Polish README for core and bindings by @Xuanwo in https://github.com/apache/opendal/pull/2554

### CI

- ci: Fix append test should use copy_buf to avoid call times by @Xuanwo in https://github.com/apache/opendal/pull/2436
- build(bindings/ruby): fix compile rb-sys on Apple M1 by @tisonkun in https://github.com/apache/opendal/pull/2451
- ci: Use summary for zig test to fix build by @Xuanwo in https://github.com/apache/opendal/pull/2480
- ci(workflow): add lua binding test workflow by @oowl in https://github.com/apache/opendal/pull/2478
- build(deps): bump actions/setup-python from 3 to 4 by @dependabot in https://github.com/apache/opendal/pull/2481
- ci(bindings/swift): add CI for Swift binding by @unixzii in https://github.com/apache/opendal/pull/2492
- ci: Try to make webhdfs tests more stable by @Xuanwo in https://github.com/apache/opendal/pull/2503
- ci(bindings/java): auto release snapshot by @tisonkun in https://github.com/apache/opendal/pull/2521
- ci: Disable the stage snapshot CI by @Xuanwo in https://github.com/apache/opendal/pull/2528
- ci: fix opendal-java snapshot releases by @tisonkun in https://github.com/apache/opendal/pull/2532
- ci: Fix typo in binding java CI by @Xuanwo in https://github.com/apache/opendal/pull/2534
- ci(bindings/swift): optimize time consumption of CI pipeline by @unixzii in https://github.com/apache/opendal/pull/2545
- ci: Fix PR label not updated while edited by @Xuanwo in https://github.com/apache/opendal/pull/2547

### Chore

- chore: Add redis bench support by @Xuanwo in https://github.com/apache/opendal/pull/2438
- chore(bindings/nodejs): update index.d.ts by @suyanhanx in https://github.com/apache/opendal/pull/2459
- chore: Add release 0.37.0 to download by @suyanhanx in https://github.com/apache/opendal/pull/2472
- chore: Fix Cargo.lock not updated by @Xuanwo in https://github.com/apache/opendal/pull/2490
- chore: Polish some code details by @Xuanwo in https://github.com/apache/opendal/pull/2505
- chore(bindings/nodejs): provide more precise type for scheme by @cijiugechu in https://github.com/apache/opendal/pull/2520

## [v0.37.0] - 2023-06-06

### Added

- feat(services/webdav): support redirection when get 302/307 response during read operation by @Yansongsongsong in https://github.com/apache/opendal/pull/2256
- feat: Add Zig Bindings Module by @kassane in https://github.com/apache/opendal/pull/2374
- feat: Implement Timeout Layer by @Xuanwo in https://github.com/apache/opendal/pull/2395
- feat(bindings/c): add opendal_operator_blocking_delete method by @jiaoew1991 in https://github.com/apache/opendal/pull/2416
- feat(services/obs): add append support by @infdahai in https://github.com/apache/opendal/pull/2422

### Changed

- refactor(bindings/zig): enable tests and more by @tisonkun in https://github.com/apache/opendal/pull/2375
- refactor(bindings/zig): add errors handler and module test by @kassane in https://github.com/apache/opendal/pull/2381
- refactor(http_util): Adopt reqwest's redirect support by @Xuanwo in https://github.com/apache/opendal/pull/2390

### Fixed

- fix(bindings/zig): reflect C interface changes by @tisonkun in https://github.com/apache/opendal/pull/2378
- fix(services/azblob): Fix batch delete doesn't work on azure by @Xuanwo in https://github.com/apache/opendal/pull/2382
- fix(services/oss): Fix oss batch max operations by @A-Stupid-Sun in https://github.com/apache/opendal/pull/2414
- fix(core): Don't wake up operator futures while not ready by @Xuanwo in https://github.com/apache/opendal/pull/2415
- fix(services/s3): Fix s3 batch max operations by @A-Stupid-Sun in https://github.com/apache/opendal/pull/2418

### Docs

- docs: service doc for s3 by @suyanhanx in https://github.com/apache/opendal/pull/2376
- docs(bindings/C): The documentation for OpenDAL C binding by @Ji-Xinyou in https://github.com/apache/opendal/pull/2373
- docs: add link for c binding by @suyanhanx in https://github.com/apache/opendal/pull/2380
- docs: docs for kv services by @suyanhanx in https://github.com/apache/opendal/pull/2396
- docs: docs for fs related services by @suyanhanx in https://github.com/apache/opendal/pull/2397
- docs(bindings/java): do not release snapshot versions anymore by @tisonkun in https://github.com/apache/opendal/pull/2398
- docs: doc for ipmfs by @suyanhanx in https://github.com/apache/opendal/pull/2408
- docs: add service doc for oss by @A-Stupid-Sun in https://github.com/apache/opendal/pull/2409
- docs: improvement of Python binding by @ideal in https://github.com/apache/opendal/pull/2411
- docs: doc for download by @suyanhanx in https://github.com/apache/opendal/pull/2424
- docs: Add release guide by @Xuanwo in https://github.com/apache/opendal/pull/2425

### CI

- ci: Enable semantic PRs by @Xuanwo in https://github.com/apache/opendal/pull/2370
- ci: improve licenserc settings by @tisonkun in https://github.com/apache/opendal/pull/2377
- build(deps): bump reqwest from 0.11.15 to 0.11.18 by @dependabot in https://github.com/apache/opendal/pull/2389
- build(deps): bump pyo3 from 0.18.2 to 0.18.3 by @dependabot in https://github.com/apache/opendal/pull/2388
- ci: Enable nextest for all behavior tests by @Xuanwo in https://github.com/apache/opendal/pull/2400
- ci: reflect ascii file rewrite by @tisonkun in https://github.com/apache/opendal/pull/2419
- ci: Remove website from git archive by @Xuanwo in https://github.com/apache/opendal/pull/2420
- ci: Add integration tests for Cloudflare R2 by @Xuanwo in https://github.com/apache/opendal/pull/2423

### Chore

- chore(bindings/python): upgrade maturin to 1.0 by @messense in https://github.com/apache/opendal/pull/2369
- chore: Fix license headers for release/labler by @Xuanwo in https://github.com/apache/opendal/pull/2371
- chore(bindings/C): add one simple read/write example into readme and code by @Ji-Xinyou in https://github.com/apache/opendal/pull/2421

## [v0.36.0] - 2023-05-30

### Added

- feat(service/fs): add append support for fs (#2296)
- feat(services/sftp): add append support for sftp (#2297)
- RFC-2299: Chain based Operator API (#2299)
- feat(services/azblob): add append support (#2302)
- feat(bindings/nodejs): add append support (#2322)
- feat(bindings/C): opendal_operator_ptr construction using kvs (#2329)
- feat(services/cos): append support (#2332)
- feat(bindings/java): implement Operator#delete (#2345)
- feat(bindings/java): support append (#2350)
- feat(bindings/java): save one jni call in the hot path (#2353)
- feat: server side encryption support for azblob (#2347)

### Changed

- refactor(core): Implement RFC-2299 for stat_with (#2303)
- refactor(core): Implement RFC-2299 for BlockingOperator::write_with (#2305)
- refactor(core): Implement RFC-2299 for appender_with (#2307)
- refactor(core): Implement RFC-2299 for read_with (#2308)
- refactor(core): Implement RFC-2299 for read_with (#2308)
- refactor(core): Implement RFC-2299 for append_with (#2312)
- refactor(core): Implement RFC-2299 for write_with (#2315)
- refactor(core): Implement RFC-2299 for reader_with (#2316)
- refactor(core): Implement RFC-2299 for writer_with (#2317)
- refactor(core): Implement RFC-2299 for presign_read_with (#2314)
- refactor(core): Implement RFC-2299 for presign_write_with (#2320)
- refactor(core): Implement RFC-2299 for list_with (#2323)
- refactor: Move `ops` to `raw::ops` (#2325)
- refactor(bindings/C): align bdd test with the feature tests (#2340)
- refactor(bindings/java): narrow unsafe boundary (#2351)

### Fixed

- fix(services/supabase): correctly set retryable (#2295)
- fix(core): appender complete check (#2298)

### Docs

- docs: add service doc for azdfs (#2310)
- docs(bidnings/java): how to deploy snapshots (#2311)
- docs(bidnings/java): how to deploy snapshots (#2311)
- docs: Fixed links of languages to open in same tab (#2327)
- docs: Adopt docusaurus pathname protocol (#2330)
- docs(bindings/nodejs): update lib desc (#2331)
- docs(bindings/java): update the README file (#2338)
- docs: add service doc for fs (#2337)
- docs: add service doc for cos (#2341)
- docs: add service doc for dashmap (#2342)
- docs(bindings/java): for BlockingOperator (#2344)

### CI

- build(bindings/java): prepare for snapshot release (#2301)
- build(bindings/java): support multiple platform java bindings (#2324)
- ci(binding/nodejs): Use docker to build nodejs binding (#2328)
- build(bindings/java): prepare for automatically multiple platform deploy (#2335)
- ci: add bindings java docs and integrate with website (#2346)
- ci: avoid copy gitignore to site folder (#2348)
- ci(bindings/c): Add diff check (#2359)
- ci: Cache librocksdb to speed up CI (#2360)
- ci: Don't load rocksdb for all workflows (#2362)
- ci: Fix Node.js 12 actions deprecated warning (#2363)
- ci: Speed up python docs build (#2364)
- ci: Adopt setup-node's cache logic instead (#2365)

### Chore

- chore(test): Avoid test names becoming prefixes of other tests (#2333)
- chore(bindings/java): improve OpenDALException tests and docs (#2343)
- chore(bindings/java): post release 0.1.0 (#2352)
- chore(docs): split docs build into small jobs (#2356)'
- chore: protect branch gh-pages (#2358)

## [v0.35.0] - 2023-05-23

### Added

- feat(services/onedrive): Implement `list`, `create_dir`, `stat` and upload
  ing large files (#2231)
- feat(bindings/C): Initially support stat in C binding (#2249)
- feat(bindings/python): Enable `abi3` to avoid building on different python
  version (#2255)
- feat(bindings/C): support BDD tests using GTest (#2254)
- feat(services/sftp): setup integration tests (#2192)
- feat(core): Add trait and public API for `append` (#2260)
- feat(services/sftp): support copy and rename for sftp (#2263)
- feat(services/sftp): support copy and read_seek (#2267)
- feat: Add COS service support (#2269)
- feat(services/cos): Add support for loading from env (#2271)
- feat(core): add presign support for obs (#2253)
- feat(services/sftp): setup integration tests (#2192)
- feat(core): add presign support for obs (#2253)
- feat(core): public API of append (#2284)
- test(core): test for append (#2286)
- feat(services/oss): add append support (#2279)
- feat(bindings/java): implement async ops to pass AsyncStepsTest (#2291)

### Changed

- services/gdrive: port code to GdriveCore & add path_2_id cache (#2203)
- refactor: Minimize futures dependencies (#2248)
- refactor: Add Operator::via_map to support init without generic type parameters (#2280)
- refactor(binding/java): build, async and docs (#2276)

### Fixed

- fix: Fix bugs that failed wasabi's integration tests (#2273)

### Removed

- feat(core): remove `scan` from raw API (#2262)

### Docs

- chore(s3): update builder region doc (#2247)
- docs: Add services in readme (#2251)
- docs: Unify capabilities list for kv services (#2257)
- docs(nodejs): fix some example code errors (#2277)
- docs(bindings/C): C binding contributing documentation (#2266)
- docs: Add new docs that available for all languages (#2285)
- docs: Remove unlicensed svg (#2289)
- fix(website): double active route (#2290)

### CI

- ci: Enable test for cos (#2270)
- ci: Add integration tests for supabase (#2272)
- ci: replace set-output for docs (#2275)
- ci: Fix unit tests (#2282)
- ci: Cleanup NOTICE file (#2281)
- ci: Fix release not contains incubating (#2292)

### Chore

- chore(core): remove unnecessary path prefix (#2265)

## [v0.34.0] - 2023-05-09

### Added

- feat(writer): configurable buffer size of unsized write (#2143)
- feat(oay): Add basic s3 list_objects_v2 with start_after support (#2219)
- feat: Add typed kv adapter and migrate moka to it (#2222)
- feat: migrate service dashmap (#2225)
- feat(services/memory): migrate service memory (#2229)
- feat: Add assert for public types to ensure Send + Sync (#2237)
- feat(services/gcs): Add abort support for writer (#2242)

### Changed

- refactor: Replace futures::ready with std::task::ready (#2221)
- refactor: Use list without delimiter to replace scan (#2243)

### Fixed

- fix(services/gcs): checked_rem_euclid could return Some(0) (#2220)
- fix(tests): Etag must be wrapped by `"` (#2226)
- fix(services/s3): Return error if credential load fail instead skip (#2233)
- fix(services/s3): Return error if region not set for AWS S3 (#2234)
- fix(services/gcs): rsa 0.9 breaks gcs presign (#2236)

### Chore

- chore: change log subscriber from env_logger to tracing-subscriber (#2238)
- chore: Fix build of wasabi (#2241)

## [v0.33.3] - 2023-05-06

### Added

- feat(services/onedrive): Add read and write support for OneDrive (#2129)
- test(core): test for `read_with_override_cache_control` (#2155)
- feat(http_util): Implement multipart/form-data support (#2157)
- feat(http_util): Implement multipart/mixed support (#2161)
- RFC-2133: Introduce Append API (#2133)
- feat(services/sftp): Add read/write/stat support for sftp (#2186)
- feat(services/gdrive): Add read & write & delete support for GoogleDrive (#2184)
- feat(services/vercel): Add vercel remote cache support (#2193)
- feat(tests): Enable supabase integration tests (#2190)
- feat(core): merge scan and list (#2214)

### Changed

- refactor(java): refactor java code for java binding (#2145)
- refactor(layers/logging): parsing level str (#2160)
- refactor: Move not initiated logic to utils instead (#2196)
- refactor(services/memcached): Rewrite memecached connection entirely (#2204)

### Fixed

- fix(service/s3): set retryable on batch (#2171)
- fix(services/supabase): Supabase ci fix (#2200)

### Docs

- docs(website): try to add opendal logo (#2159)
- doc: update vision to be more clear (#2164)
- docs: Refactor `Contributing` and add `Developing` (#2169)
- docs: Merge DEVELOPING into CONTRIBUTING (#2172)
- docs: fix some grammar errors in the doc of Operator (#2173)
- docs(nodejs): Add CONTRIBUTING docs (#2174)
- docs: Add CONTRIBUTING for python (#2188)

### CI

- ci: Use microsoft rust devcontainer instead (#2165)
- ci(devcontainer): Install development deps (#2167)
- chore: set workspace default members (#2168)
- ci: Setup vercel artifacts integration tests (#2197)
- ci: Remove not used odev tools (#2202)
- ci: Add tools to generate NOTICE and all deps licenses (#2205)
- ci: use Temurin JDK 11 to build the bindings-java (#2213)

### Chore

- chore(deps): bump clap from 4.1.11 to 4.2.5 (#2183)
- chore(deps): bump futures from 0.3.27 to 0.3.28 (#2181)
- chore(deps): bump assert_cmd from 2.0.10 to 2.0.11 (#2180)
- chore: Refactor behavior test (#2189)
- chore: update readme for more information that is more friendly to newcomers (#2217)

## [v0.33.2] - 2023-04-27

### Added

- feat(core): add test for `stat_with_if_none_match` (#2122)
- feat(services/gcs): Add start-after support for list (#2107)
- feat(services/azblob): Add supporting presign (#2120)
- feat(services/gcs): Add supporting presign support (#2126)
- feat(java): connect rust async/await with java future (#2112)
- docs: add hdfs classpath related troubleshoot (#2130)
- fix(clippy): suppress dead_code check (#2135)
- feat(core): Add `cache-control` to Metadata (#2136)
- fix(services/gcs): Remove HOST header to avoid gcs RESET connection (#2139)
- test(core): test for `write_with_cache_control` (#2131)
- test(core): test for `write_with_content_type` (#2140)
- test(core): test for `read_with_if_none_match` (#2141)
- feat(services/supabase): Add read/write/stat support for supabase (#2119)

### Docs

- docs: add hdfs classpath related troubleshoot (#2130)

### CI

- ci: Mark job as skipped if owner is not apache (#2128)
- ci: Enable native-tls to test gcs presign for workaround (#2138)

## [v0.33.1] - 2023-04-25

### Added

- feat: Add behavior test for read_with_if_match & stat_with_if_match (#2088)
- feat(tests): Add fuzz test for writer without content length (#2100)
- feat: add if_none_match support for obs (#2103)
- feat(services/oss): Add server side encryption support for oss (#2092)
- feat(core): update errorKind `PreconditionFailed` to `ConditionNotMatch` (#2104)
- feat(services/s3): Add `start-after` support for list (#2096)
- feat: gcs support cache control (#2116)

### Fixed

- fix(services/gcs): set `content length=0` for gcs initiate_resumable_upload (#2110)
- fix(bindings/nodejs): Fix index.d.ts not updated (#2117)

### Docs

- chore: improve LoggingLayer docs and pub use log::Level (#2089)
- docs(refactor): Add more detailed description of operator, accessor, and builder (#2094)

### CI

- chore(bindings/nodejs): update `package.json` repository info (#2078)
- ci: Bring hdfs test back (#2114)

## [v0.33.0] - 2023-04-23

### Added

- feat: Add OpenTelemetry Trace Layer (#2001)
- feat: add if_none_match support for azblob (#2035)
- feat: add if_none_match/if_match for gcs (#2039)
- feat: Add size check for sized writer (#2038)
- feat(services/azblob): Add if-match support (#2037)
- feat(core): add copy&rename to error_context layer (#2040)
- feat: add if-match support for OSS (#2034)
- feat: Bootstrap new (old) project oay (#2041)
- feat(services/OSS): Add override_content_disposition support (#2043)
- feat: add IF_MATCH for http (#2044)
- feat: add IF_MATCH for http HEAD request (#2047)
- feat: add cache control header for azblob and obs (#2049)
- feat: Add capability for operation's variant and args (#2057)
- feat(azblob): Add override_content_disposition support (#2065)
- feat(core): test for read_with_override_content_composition (#2067)
- feat(core): Add `start-after` support for list (#2071)

### Changed

- refactor: Polish Writer API by merging append and write together (#2036)
- refactor(raw/http_util): Add url in error context (#2066)
- refactor: Allow reusing the same operator to speed up tests (#2068)

### Fixed

- fix(bindings/ruby): use rb_sys_env to help find ruby for building (#2051)
- fix: MadsimLayer should be able to built without cfg (#2059)
- fix(services/s3): Ignore prefix if it's empty (#2064)

### Docs

- docs(bindings/python): ipynb examples for users (#2061)

### CI

- ci(bindings/nodejs): publish support `--provenance` (#2046)
- ci: upgrade typos to 1.14.8 (#2055)
- chore(bindings/C): ignore the formatting of auto-generated opendal.h (#2056)

## [v0.32.0] - 2023-04-18

### Added

- feat: Add wasabi service implementation (#2004)
- feat: improve the readability of oli command line error output (#2016)
- feat: add If-Match Support for OpRead, OpWrite, OpStat (#2017)
- feat: add behavioral test for Write::abort (#2018)
- feat: add if-match support for obs (#2023)
- feat: Add missing functions for trace layers (#2025)
- feat(layer): add madsim layer (#2006)
- feat(gcs): add support for gcs append (#1801)

### Changed

- refactor: Rename `Create` to `CreateDir` for its behavior changed (#2019)

### Fixed

- fix: Cargo lock not updated (#2027)
- fix(services/s3): Ignore empty query to make it more compatible (#2028)
- fix(services/oss): Fix env not loaded for oss signer (#2029)

### Docs

- docs: fix some typos (#2022)
- docs: add dev dependency section (#2021)

## [v0.31.1] - 2023-04-17

### Added

- feat(services/azdfs): support rename (#1929)
- test: Increate copy/move nested path test depth (#1932)
- feat(layers): add a basic minitrace layer (#1931)
- feat: add Writer::abort method (#1937)
- feat(services/gcs): Allow setting PredefinedAcl (#1989)
- feat(services/oss): add oss cache-control header support (#1986)
- feat: Add PreconditionFailed Error so users can handle them (#1993)
- feat: add http if_none_match support (#1995)
- feat: add oss if-none-match support (#1997)
- feat(services/gcs): Allow setting default storage_class (#1996)
- feat(binding/C): add clang-format for c binding (#2003)

### Changed

- refactor: Polish the behavior of scan (#1926)
- refactor: Polish the implementation of webhdfs (#1935)

### Fixed

- fix: sled should not be enabled by default (#1923)
- fix: kv adapter's writer implementation fixed to honour empty writes (#193

4.

- fix(services/azblob): fix copy missing content-length (#2000)

### Docs

- docs: Adding docs link to python binding (#1921)
- docs(bindings/python): fix wrong doc link (#1928)
- docs: Add contributing for OpenDAL (#1984)
- docs: Add explanation in contributing (#1985)
- docs: Feel relax in community and don't hurry (#1987)
- docs: update contributing (#1998)
- docs(services/memory): Fix wrong explanation (#2002)
- docs: Add OpenDAL VISION (#2007)
- docs: update VISION and behavior tests doc (#2009)

### CI

- ci(bindings/nodejs): Access should be set to public before publish (#1919)
- ci: Re-enable webhdfs test (#1925)
- chore: add .editorconfig (#1988)
- ci: Fix format after adding editorconfig (#1990)

## [v0.31.0] - 2023-04-12

### Added

- feat(bindings/java): add cucumber test (#1809)
- feat(bindings/java): setup Java CI (#1823)
- feat: add if_none_match support (#1832)
- feat: Retry when some of batch operations are failing (#1840)
- feat: add writer support for aliyun oss (#1842)
- feat(core): Add Copy Support (#1841)
- feat(bindings/c): fix c bindings makefile (#1849)
- feat(core): add behavior tests for copy & blocking_copy (#1852)
- feat(s3): allow users to specify storage_class (#1854)
- feat(s3): Support copy (#1856)
- Add check for s3 bucket name (#1857)
- feat(core): Support rename (#1862)
- feat(bindings/nodejs): add `copy` and `rename` (#1866)
- feat(azblob): Support copy (#1868)
- feat(gcs): copy support for GCS (#1869)
- feat(bindings/c): framework of add basic io and init logics (#1861)
- feat(webdav): support copy (#1870)
- feat(services/oss): Add Copy Support (#1874)
- feat(services/obs): Add Copy Support (#1876)
- feat(services/webdav): Support Rename (#1878)
- binding/c: parse opendal to use typed BlockingOperator (#1881)
- binding/c: clean up comments and type assertion for BlockingOperator (#1883)
- binding(python): Support python binding benchmark for opendal (#1882)
- feat(bindings/c): add support for free heap-allocated operator (#1890)
- feat(binding/c): add is_exist to operator (#1892)
- feat(bindings/java): add Stat support (#1894)
- feat(services/gcs): Add customed token loader support (#1908)
- feat(services/oss): remove unused builder prop allow_anonymous (#1913)
- feat: Add feature flag for all services (#1915)

### Changed

- refactor(core): Simplify the usage of BatchOperation and BatchResults (#1843)
- refactor: Use reqwest blocking client instead of ureq (#1853)
- refactor: Bump MSRV to 1.64 (#1864)
- refactor: Remove not used blocking http client (#1895)
- refactor: Change presign to async for future refactor (#1900)
- refactor(services/gcs): Migrate to async reqsign (#1906)
- refactor(services/azdfs): Migrate to async reqsign (#1903)
- refactor(services/azblob): Adopt new reqsign (#1902)
- refactor(services/s3): Migrate to async reqsign (#1909)
- refactor(services/oss): Migrate to async reqsign (#1911)
- refactor: Use chrono instead of time to work well with ecosystem (#1912)
- refactor(service/obs): Migrate obs to async reqsign (#1914)

### Fixed

- fix: podling website check (#1838)
- fix(website): copyright update (#1839)
- fix(core): Add checks before doing copy (#1845)
- fix(core): S3 Copy should set SSE headers (#1860)
- fix: Fix presign related unit tests (#1910)

### Docs

- docs(bindings/nodejs): fix build failed (#1819)
- docs: fix several typos in the documentation (#1846)
- doc(bindings/nodejs): update presign example in doc (#1901)

### CI

- ci: Fix build for nodejs binding on macos (#1813)
- binding/c: build: add phony to makefile, and some improve (#1850)
- ci: upgrade hawkeye action (#1834)

### Chore

- chore(bindings/nodejs): add deno benchmark (#1814)
- chore: Add CODEOWNERS (#1817)
- chore(deps): bump opentelemetry-jaeger from 0.16.0 to 0.18.0 (#1829)
- chore(deps): bump opentelemetry from 0.17.0 to 0.19.0 (#1830)
- chore(deps): bump tokio from 1.26.0 to 1.27.0 (#1828)
- chore(deps): bump napi-derive from 2.12.1 to 2.12.2 (#1827)
- chore(deps): bump async-trait from 0.1.67 to 0.1.68 (#1826)
- chore: Cleanup code for oss writer (#1847)
- chore: Make clippy happy (#1865)
- binding(python): Format python code in binding (#1885)

## [v0.30.5] - 2023-03-31

### Added

- feat(oli): implement `oli rm` (#1774)
- feat(bindings/nodejs): Support presign (#1772)
- feat(oli): implement `oli stat` (#1778)
- feat(bindings/object_store): Add support for list and list_with_delimiter (#1784)
- feat(oli): implement `oli cp -r` (#1787)
- feat(bindings/nodejs): Make PresignedRequest serializable (#1797)
- feat(binding/c): add build.rs and cbindgen as dep to gen header (#1793)
- feat(bindings/nodejs): Add more APIs and examples (#1799)
- feat: reader_with and writer_with (#1803)
- feat: add override_cache_control (#1804)
- feat: add cache_control to OpWrite (#1806)

### Changed

- refactor(oli): switch to `Operator::scan` and `Operator::remove_all` (#1779)
- refactor(bindings/nodejs): Polish benchmark to make it more readable (#1810)

### Fixed

- fix(oli): set the root of fs service to '/' (#1773)
- fix: align WebDAV stat with RFC specification (#1783)
- fix(bindings/nodejs): fix read benchmark (#1805)

### CI

- ci: Split clippy and docs check (#1785)
- ci(bindings/nodejs): Support aarch64-apple-darwin (#1780)
- ci(bindings/nodejs): publish with LICENSE & NOTICE (#1790)
- ci(services/redis): Add dragonfly test (#1808)

### Chore

- chore(bindings/python): update maturin to 0.14.16 (#1777)
- chore(bin/oli): Set oli version from package version (#1786)
- chore(oli): set cli version in a central place (#1789)
- chore: don't pin time version (#1788)
- chore(bindings/nodejs): init benchmark (#1798)
- chore(bindings/nodejs): Fix generated headers (#1802)

## [v0.30.4] - 2023-03-26

### Added

- feat(oli): add config file to oli (#1706)
- feat: make oli support more services (#1717)
- feat(bindings/ruby): Setup the integrate with magnus (#1712)
- feat(bindings/ruby): setup cucumber tests (#1725)
- feat(bindings/python): convert to mixed Python/Rust project layout (#1729)
- RFC-1735: Operation Extension (#1735)
- feat(oli): load config from both env and file (#1737)
- feat(bindings/ruby): support read and write (#1734)
- feat(bindings/ruby): support stat, and pass all blocking bdd test (#1743)
- feat(bindings/ruby): add namespace (#1745)
- feat: Add override_content_disposition for OpRead (#1742)
- feat(bindings/java): add java binding (#1736)
- feat(oli): implement oli ls (#1755)
- feat(oli): implement oli cat (#1759)

### Fixed

- fix(bindings/nodejs): Publish sub-package name (#1704)

### Docs

- docs: Update comparison vs object_store (#1698)
- docs(bindings/python): add pdoc to docs env (#1718)
- docs: List working on bindings in README (#1753)

### CI

- ci: Fix workflow not triggered when itself changed (#1716)
- ci: Remove ROCKSDB_LIB_DIR after we didn't install librocksdb (#1719)
- ci: Fix nodejs built failed for "Unexpected token o in JSON at position 0" (#1722)
- ci: Split cache into more parts (#1730)
- ci: add a basic ci for ruby (#1744)
- ci: Remove target from cache (#1764)

### Chore

- chore: Fix CHANGELOG not found (#1694)
- chore: Remove publish=false of oli (#1697)
- chore: Fix a few typos in code comment (#1701)
- chore(bindins/nodejs): Update README (#1709)
- chore: rename binaries to bin (#1714)
- chore: bump rocksdb to resolve dependency conflicts with magnus (#1713)
- chore(bindings/nodejs): Remove outdated napi patches (#1727)
- chore: Add CITATION file for OpenDAL (#1746)
- chore: improve NotADirectory error message with ending slash (#1756)
- chore(bindings/python): update pyo3 to 0.18.2 (#1758)

## [v0.30.3] - 2023-03-16

### Added

- feat: Infer storage name based on endpoint (#1551)
- feat(bindings/python): implement async file-like reader API (#1570)
- feat: website init (#1580)
- feat(bindings/python): implement list and scan for AsyncOperator (#1586)
- feat: Implement logging/metrics/tracing support for Writer/BlockingWriter (#1588)
- feat(bindings/python): expose layers to Python (#1591)
- feat(bindings/c): Setup the integrate with cbindgen (#1603)
- feat(bindings/nodejs): Auto-generate docs (#1625)
- feat: add max_batch_operations for AccessorInfo (#1615)
- feat(azblob): Add support for batch operations (#1610)
- services/redis: Implement Write::append with native support (#1651)
- feat(tests): Introducing BDD tests for all bindings (#1654)
- feat(bindings/nodejs): Migrate to BDD test (#1661)
- feat(bindings/nodejs): Add generated `index.d.ts` (#1664)
- feat(bindings/python): add auto-generated api docs (#1613)
- feat(bindings/python): add `__repr__` to `Operator` and `AsyncOperator` (#1683)

### Changed

- \*: Change all files licenses to ASF (#1592)
- refactor(bindings/python): only enable `pyo3/entension-module` feature when building with maturin (#1680)

### Fixed

- fix(bindings/python): Fix the metadata for Python binding (#1568)
- fix: Operator::remove_all behaviour on non-existing object fixed (#1587)
- fix: reset Reader::SeekState when poll completed (#1609)
- fix: Bucket config related error is misleadling (#1684)
- fix(services/s3): UploadId should be percent encoded (#1690)

### CI

- ci: Fix typo in workflows (#1582)
- ci: Don't check dep updates so frequently (#1599)
- ci: Setup asf config (#1622)
- ci: Use gh-pages instead (#1623)
- ci: Update Github homepage (#1627)
- ci: Update description for OpenDAL (#1628)
- ci: Send notifications to commits@o.a.o (#1629)
- ci: set main branch to be protected (#1631)
- ci: Add release scripts for OpenDAL (#1637)
- ci: Add check scripts (#1638)
- ci: Remove rust-cache to allow we can test rust code now (#1643)
- ci: Enable license check back (#1663)
- ci(bindings/nodejs): Enable formatter (#1665)
- ci: Bring our actions back (#1668)
- ci: Use korandoru/hawkeye@v1.5.4 instead (#1672)
- ci: Fix license header check and doc check (#1674)
- infra: Add odev to simplify contributor's setup (#1687)

### Docs

- docs: Migrate links to o.a.o (#1630)
- docs: update the old address and the LICENSE size. (#1633)
- doc: update doc-link (#1642)
- docs(blog): Way to Go: OpenDAL successfully entered Apache Incubator (#1652)
- docs: Reorganize README of core and whole project (#1676)
- doc: Update README.md for quickstart (#1650)
- doc: uncomment the use expr for operator example (#1685)

### Website

- website: Let's deploy our new website (#1581)
- website: Fix CNAME not set (#1590)
- website: Fix website publish (#1626)
- website: Add GitHub entry (#1636)
- website: move some content of footer to navbar. (#1660)

### Chore

- chore(bindings/nodejs): fix missing files to publish (#1569)
- chore(deps): bump lazy-regex from 2.4.1 to 2.5.0 (#1573)
- chore(deps): bump tokio from 1.25.0 to 1.26.0 (#1577)
- chore(deps): bump hyper from 0.14.24 to 0.14.25 (#1575)
- chore(deps): bump serde from 1.0.152 to 1.0.155 (#1576)
- chore(deps): bump peaceiris/actions-gh-pages from 3.9.0 to 3.9.2 (#1593)
- chore(deps): bump async-trait from 0.1.64 to 0.1.66 (#1594)
- chore(deps): bump serde_json from 1.0.93 to 1.0.94 (#1596)
- chore(deps): bump paste from 1.0.11 to 1.0.12 (#1598)
- chore(deps): bump napi from 2.11.2 to 2.11.3 (#1595)
- chore(deps): bump serde from 1.0.155 to 1.0.156 (#1600)
- chore: fix the remaining license (#1605)
- chore: add a basic workflow for c bindings (#1608)
- chore: manage deps with maturin (#1611)
- chore: Rename files to yaml (#1624)
- chore: remove PULL_REQUEST_TEMPLATE (#1634)
- chore: add NOTICE and DISCLAIMER (#1635)
- chore(operator): apply max_batch_limit for async operator (#1641)
- chore: replace datafuselabs/opendal with apache/opendal (#1647)
- chore: make check.sh be executable and update gitignore (#1648)
- chore(automation): fix release.sh packaging sha512sum (#1649)
- chore: Update metadata (#1666)
- chore(website): Remove authors.yml (#1669)
- chore: Move opendal related staffs to core (#1673)
- chore: Remove not needed ignore from licenserc (#1677)
- chore: Ignore generated docs from git (#1686)

## [v0.30.2] - 2023-03-10

### CI

- ci(bindings/nodejs): Fix nodejs package can't uploaded (#1564)

## [v0.30.1] - 2023-03-10

### Docs

- docs: Fix Operator::create() has been removed (#1560)

### CI

- ci: Fix python & nodejs not released correctly (#1559)

### Chore

- chore(bindings/nodejs): update license in package.json (#1556)

## [v0.30.0] - 2023-03-10

### Added

- RFC-1477: Remove Object Concept (#1477)
- feat(bindings/nodejs): fs Operator (#1485)
- feat(service/dashmap): Add scan support (#1492)
- feat(bindings/nodejs): Add Writer Support (#1490)
- feat: Add dummy implementation for accessor and builder (#1503)
- feat(bindings/nodejs): Support List & append all default services (#1505)
- feat(bindings/python): Setup operator init logic (#1513)
- feat(bindings/nodejs): write support string (#1520)
- feat(bindings/python): add support for services that opendal enables by default (#1522)
- feat(bindings/nodejs): Remove Operator.writer until we are ready (#1528)
- feat(bindings/nodejs): Support Operator.create_dir (#1529)
- feat(bindings/python): implement create_dir (#1534)
- feat(bindings/python): implement delete and export more metadata fields (#1539)
- feat(bindings/python): implement blocking list and scan (#1541)
- feat: Append EntryMode to Entry (#1543)
- feat: Entry refactoring to allow external creation (#1547)
- feat(bindings/nodejs): Support Operator.scanSync & Operator.listSync (#1546)
- feat: remove_via can delete files concurrently (#1495)

### Changed

- refactor: Split operator APIs into different part (#1483)
- refactor: Remove Object prefix for public API (#1488)
- refactor: Remove the concept of Object (#1496)
- refactor: remove ReadDir in FTP service (#1504)
- refactor: rename public api create to create_dir (#1512)
- refactor(bindings/python): return bytes directly and add type stub file (#1514)
- tests: Remove not needed create file test (#1516)
- refactor: improve the python binding implementation (#1517)
- refactor(bindings/nodejs): Remove scheme from bindings (#1552)

### Fixed

- fix(services/s3): Make sure the ureq's body has been consumed (#1497)
- fix(services/s3): Allow retry error RequestTimeout (#1532)

### Docs

- docs: Remove all references to object (#1500)
- docs(bindings/python): Add building docs (#1526)
- docs(bindings/nodejs): update readme (#1527)
- docs: Add detailed docs for create_dir (#1537)

### CI

- ci: Don't run binding tests if only services changes (#1498)
- ci: Improve rocksdb build speed by link dynamic libs (#1502)
- ci: Fix bindings CI not running on PR (#1530)
- ci: Polish scripts and prepare for releasing (#1553)

### Chore

- chore: Re-organize the project layout (#1489)
- chore: typo & clippy (#1499)
- chore: typo (#1501)
- chore: Move memcache-async into opendal (#1544)

## [v0.29.1] - 2023-03-05

### Added

- feat(bindings/python): Add basic IO support (#1464)
- feat(binding/node.js): basic IO (#1416)
- feat(bindings/nodejs): Align to OpenDAL exports (#1466)
- chore(bindings/nodejs): remove duplicate attribute & unused comment (#1478)

### Changed

- refactor: Promote operator as a mod for further refactor (#1479)

### Docs

- docs: Add convert from m\*n to m+n (#1454)
- docs: Polish comments for public types (#1455)
- docs: Add discord chat link (#1474)

### Chore

- chore: fix typo (#1456)
- chore: fix typo (#1459)
- benches: Generate into Bytes instead (#1463)
- chore(bindings/nodjes): Don't check-in binaries (#1469)
- chore(binding/nodejs): specific package manager version with hash (#1470)

## [v0.29.0] - 2023-03-01

### Added

- RFC-1420: Object Writer (#1420)
- feat: oss backend support http protocol (#1432)
- feat: Implement ObjectWriter Support (#1431)
- feat/layers/retry: Add Write Retry support (#1447)
- feat: Add Write append tests (#1448)

### Changed

- refactor: Decouple decompress read feature from opendal (#1406)
- refactor: Cleanup pager related implementation (#1439)
- refactor: Polish the implement details for Writer (#1445)
- refactor: Remove `io::input` and Rename `io::output` to `oio` (#1446)

### Fixed

- fix(services/s3): Fix part number for AWS S3 (#1450)

### CI

- ci: Consistently apply license header (#1411)
- ci: add typos check (#1425)

### Docs

- docs: Add services-dashmap feature (#1404)
- docs: Fix incorrect indent for title (#1405)
- docs: Add internal sections of Accessor and Layer (#1408)
- docs: Add more guide for Accessor (#1409)
- docs: Add tutorial of building a duck storage service (#1410)
- docs: Add a basic object example (#1422)

### Chore

- chore: typo fix (#1418)
- chore: Make license check happy (#1423)
- chore: typo-fix (#1434)

## [v0.28.0] - 2023-02-22

### Added

- feat: add dashmap support (#1390)

### Changed

- refactor: Implement query based object metadata cache (#1395)
- refactor: Store complete inside bits and add more examples (#1397)
- refactor: Trigger panic if users try to visit not fetched metadata (#1399)
- refactor: Polish the implement of Query Based Metadata Cache (#1400)

### Docs

- RFC-1391: Object Metadataer (#1391)
- RFC-1398: Query Based Metadata (#1398)

## [v0.27.2] - 2023-02-20

### Added

- feat: Add batch API for Accessor (#1339)
- feat: add Content-Disposition for inner API (#1347)
- feat: add content-disposition support for services (#1350)
- feat: webdav service support bearer token (#1349)
- feat: support auth for HttpBackend (#1359)
- feat: Add batch delete support (#1357)
- feat(webdav): add list and improve create (#1330)
- feat: Integrate batch with existing ecosystem better (#1378)
- feat: Add batch delete support for oss (#1385)

### Changed

- refactor: Authorization logic for WebdavBackend (#1348)
- refactor(webhdfs): handle 307 redirection instead of noredirect (#1358)
- refactor: Polish http authorization related logic (#1367)
- refactor: Cleanup duplicated code (#1373)
- refactor: Cleanup some not needed error context (#1374)

### Docs

- docs: Fix broken links (#1344)
- docs: clarify about opendal user defined client (#1356)

### Fixed

- fix(webhdfs): should prepend http:// scheme (#1354)

### Infra

- ci: Pin time <= 0.3.17 until we decide to bump MSRV (#1361)
- ci: Only run service test on changing (#1363)
- ci: run tests with nextest (#1370)

## [v0.27.1] - 2023-02-13

### Added

- feat: Add username and password support for WebDAV (#1323)
- ci: Add test case for webdav with basic auth (#1327)
- feat(oli): support s3 uri without profile (#1328)
- feat: Add scan support for kv adapter (#1333)
- feat: Add scan support for sled (#1334)

### Changed

- chore(deps): update moka requirement from 0.9 to 0.10 (#1331)
- chore(deps): update rocksdb requirement from 0.19 to 0.20 (#1332)

### Fixed

- fix(services/oss,s3): Metadata should be marked as complete (#1335)

## [v0.27.0] - 2023-02-11

### Added

- feat: Add Retryable Pager Support (#1304)
- feat: Add Sled support (#1305)
- feat: Add Object::scan() support (#1314)
- feat: Add object page size support (#1318)

### Changed

- refactor: Hide backon from our public API (#1302)
- refactor: Don't expose ops structs to users directly (#1303)
- refactor: Move and rename ObjectPager and ObjectEntry for more clear semantics (#1308)
- refactor: Implement strong typed pager (#1311)
- deps: remove unused deps (#1321)
- refactor: Extract scan as a new API and remove ListStyle (#1324)

### Docs

- docs: Add risingwave in projects (#1322)

### Fixed

- ci: Fix dev container Dockerfile (#1298)
- fix: Rocksdb's scheme not output correctly (#1300)
- chore: fix name typo in oss backend (#1316)
- chore: Add typos-cli and fix typos (#1320)

## [v0.26.2] - 2023-02-07

### Added

- feat: Add ChaosLayer to inject errors into underlying services (#1287)
- feat: Implement retry reader (#1291)
- feat: use std::path::Path for fs backend (#1100)
- feat: Implement services webhdfs (#1263)

### Changed

- refactor: Split CompleteReaderLayer from TypeEraserLayer (#1290)
- refactor(services/fs): Remove not needed generic (#1292)

### Docs

- docs: fix typo (#1285)
- docs: Polish docs for better reading (#1288)

### Fixed

- fix: FsBuilder can't be used with empty root anymore (#1293)
- fix: Fix retry happened in seek's read ahead logic (#1294)

## [v0.26.1] - 2023-02-05

### Changed

- refactor: Remove not used layer subdir (#1280)

### Docs

- docs: Add v0.26 upgrade guide (#1276)
- docs: Add feature sets in services (#1277)
- docs: Migrate all docs in rustdoc instead (#1281)
- docs: Fix index page not redirected (#1282)

## [v0.26.0] - 2023-02-04

### Added

- feat: Add benchmarks for blocking_seek operations (#1258)
- feat: add dev container (#1261)
- feat: Zero Cost OpenDAL (#1260)
- feat: Allow dynamic dispatch layer (#1273)

### Changed

- refactor: remove the duplicated dependency in dev-dependencies (#1257)
- refactor: some code in GitHub Actions (#1269)
- refactor: Don't expose services mod directly (#1271)
- refactor: Polish Builder API (#1272)

## [v0.25.2] - 2023-01-30

### Added

- feat: Add basic object_store support (#1243)
- feat: Implement webdav support (#1246)
- feat: Allow passing content_type to OSS presign (#1252)
- feat: Make sure short functions have been inlined (#1253)

### Changed

- refacor(services/fs): Make normalized path check optional (#1242)

### Docs

- docs(http): remove out-dated comments (#1240)
- docs: Add bindings in README (#1244)
- docs: Add docs for webdav and http services (#1248)
- docs: Add webdav in lib docs (#1249)

### Fixed

- fix(services/ghac): Fix log message for ghac_upload in write (#1239)

## [v0.25.1] - 2023-01-27

### Added

- ci: Setup benchmark workflow (#1200)
- feat: Let's try play with python (#1205)
- feat: Let's try play with Node.js (#1206)
- feat: Allow retry sending read request (#1212)
- ci: Make sure opendal is buildable on windows (#1221)
- ci: Remove not needed audit checks (#1226)

### Changed

- refactor: Remove observe read/write (#1202)
- refactor: Remove not used unwind safe feature (#1218)
- cleanup: Move oli and oay into binaries (#1227)
- cleanup: Move testdata into tests/data (#1228)
- refactor(layers/metrics): Defer initiation of error counters (#1232)

### Fixed

- fix: Retry for read and write should at ObjectReader level (#1211)

## [v0.25.0] - 2023-01-18

### Added

- feat: Add dns cache for std dns resolver (#1191)
- feat: Allow setting http client that built from external (#1192)
- feat: Implement BlockingObjectReader (#1194)

### Changed

- chore(deps): replace dotenv with dotenvy (#1187)
- refactor: Avoid calling detect region if we know the region (#1188)
- chore: ensure minimal version buildable (#1193)

## [v0.24.6] - 2023-01-12

### Added

- feat: implement tokio::io::{AsyncRead, AsyncSeek} for ObjectReader (#1175)
- feat(services/hdfs): Evaluating the new async implementation (#1176)
- feat(services/ghac): Handling too many requests error (#1181)

### Fixed

- doc: fix name change in README (#1179)

## [v0.24.5] - 2023-01-09

### Fixed

- fix(services/memcached): TcpStream should only accept host:port (#1170)

## [v0.24.4] - 2023-01-09

### Added

- feat: Add presign endpoint option for OSS (#1135)
- feat: Reset state while returning error so that we can retry IO (#1166)

### Changed

- chore(deps): update base64 requirement from 0.20 to 0.21 (#1164)

### Fixed

- fix: Memcached can't work on windows (#1165)

## [v0.24.3] - 2023-01-09

### Added

- feat: Implement memcached service support (#1161)

## [v0.24.2] - 2023-01-08

### Changed

- refactor: Use dep: to make our features more clean (#1153)

### Fixed

- fix: ghac shall return ObjectAlreadyExists for writing the same path (#1156)
- fix: futures read_to_end will lead to performance regression (#1158)

## [v0.24.1] - 2023-01-08

### Fixed

- fix: Allow range_read to be retired (#1149)

## [v0.24.0] - 2023-01-07

### Added

- Add support for SAS tokens in Azure blob storage (#1124)
- feat: Add github action cache service support (#1111)
- docs: Add docs for ghac service (#1126)
- feat: Implement offset seekable reader for zero cost read (#1133)
- feat: Implement fuzz test on ObjectReader (#1140)

### Changed

- chore(deps): update quick-xml requirement from 0.26 to 0.27 (#1101)
- ci: Enable rust cache for CI (#1107)
- deps(oay,oli): Update dependences of oay and oli (#1122)
- refactor: Only add content length hint if we already know length (#1123)
- refactor: Redesign outpu bytes reader trait (#1127)
- refactor: Remove open related APIs (#1129)
- refactor: Merge and cleanup io & io_util modules (#1136)

### Fixed

- ci: Fix build for oay and oli (#1097)
- fix: Fix rustls support for suppaftp (#1102)
- fix(services/ghac): Fix pkg version not used correctly (#1125)

## [v0.23.0] - 2022-12-22

### Added

- feat: Implement object handler so that we can do seek on file (#1091)
- feat: Implement blocking for hdfs (#1092)
- feat(services/hdfs): Implement open and blocking open (#1093)
- docs: Add mozilla/sccache into projects (#1094)

## [v0.22.6] - 2022-12-20

### Added

- feat(io): make BlockingBytesRead Send + Sync (#1083)
- feat(fs): skip seek if offset is 0 (#1082)
- RFC-1085: Object Handler (#1085)
- feat(services/s3,gcs): Allow accepting signer directly (#1087)

## [v0.22.5] - 2022-12-13

### Added

- feat: Add service account support for gcs (#1076)

## [v0.22.4] - 2022-12-13

### Added

- improve blocking read use read_to_end (#1072)
- feat(services/gcs): Fully implement default credential support (#1073)

### Fixed

- fix: read a large range without error and add test (#1068)
- fix(services/oss): Enable standard behavior for oss range (#1070)

## [v0.22.3] - 2022-12-11

### Added

- feat(layers/metrics): Merge error and failure counters together (#1058)
- feat: Set MSRV to 1.60 (#1060)
- feat: Add unwind safe flag for operator (#1061)
- feat(azblob): Add build from connection string support (#1064)

### Fixed

- fix(services/moka): Don't print all content in cache (#1057)

## [v0.22.2] - 2022-12-07

### Added

- feat(presign): support presign head method for s3 and oss (#1049)

## [v0.22.1] - 2022-12-05

### Fixed

- fix(services/s3): Allow disable loading from imds_v2 and assume_role (#1044)

## [v0.22.0] - 2022-12-05

### Added

- feat: improve temp file organization when enable atomic write in fs (#1017)
- feat: Allow configure LoggingLayer's level (#1021)
- feat: Enable users to specify the cache policy (#1024)
- feat: Implement presign for oss (#1035)

### Changed

- refactor: Polish error handling of different services (#1018)
- refactor: Merge metadata and content cache together (#1020)
- refactor(layer/cache): Allow users implement cache by themselves (#1040)

### Fixed

- fix(services/fs): Make sure writing file is truncated (#1036)

## [v0.21.2] - 2022-11-27

### Added

- feat: Add azdfs support (#1009)
- feat: Set MSRV of opendal to 1.60 (#1012)

### Docs

- docs: Fix docs for azdfs service (#1010)

## [v0.21.1] - 2022-11-26

### Added

- feat: Export ObjectLister as public type (#1006)

### Changed

- deps: Remove not used thiserror and num-trait (#1005)

## [v0.21.0] - 2022-11-25

### Added

- docs: Add greptimedb and mars into projects (#975)
- RFC-0977: Refactor Error (#977)
- feat: impl atomic write for fs service (#991)
- feat: Add OperatorMetadata to avoid expose AccessorMetadata (#997)
- feat: Improve display for error (#1002)

### Changed

- refactor: Use separate Error instead of std::io::Error to avoid confusing (#976)
- refactor: Return ReplyCreate for create operation (#981)
- refactor: Add ReplyRead for read operation (#982)
- refactor: Add RpWrite for write operation (#983)
- refactor: Add RpStat for stat operation (#984)
- refactor: Add RpDelete for delete operations (#985)
- refactor: Add RpPresign for presign operation (#986)
- refactor: Add reply for all multipart operations (#988)
- refactor: Add Reply for all blocking operations (#989)
- refactor: Avoid accessor in object entry (#992)
- refactor: Move accessor into raw apis (#994)
- refactor: Move io to raw (#996)
- refactor: Move {path,wrapper,http_util,io_util} into raw modules (#998)
- refactor: Move ObjectEntry and ObjectPage into raw (#999)
- refactor: Accept Operator instead of `Arc<dyn Accessor>` (#1001)

### Fixed

- fix: RetryAccessor is too verbose (#980)

## [v0.20.1] - 2022-11-18

### Added

- feat: Implement blocking operations for cache services (#970)

### Fixed

- fix: Use std Duration as args instead (#966)
- build: Make opendal buildable on 1.60 (#968)
- fix: Avoid cache missing after write (#971)

## [v0.20.0] - 2022-11-17

### Added

- RFC-0926: Object Reader (#926)
- feat: Implement Object Reader (#928)
- feat(services/s3): Return Object Meta for Read operation (#932)
- feat: Implement Bytes Content Range (#933)
- feat: Add Content Range support in ObjectMetadata (#935)
- feat(layers/content_cache): Implement WholeCacheReader (#936)
- feat: CompressAlgorithm derive serde. (#939)
- feat: Allow using opendal without tls support (#945)
- refactor: Refactor OpRead with BytesRange (#946)
- feat: Allow using opendal with native tls support (#949)
- docs: add docs for tls dependencies features (#951)
- feat: Make ObjectReader content_length returned for all services (#954)
- feat(layers): Implement fixed content cache (#953)
- feat: Enable default_ttl support for redis (#960)

### Changed

- refactor: Return ObjectReader in Accessor::read (#929)
- refactor(oay,oli): drop unnecessary patch.crates-io from `Cargo.toml`
- refactor: Polish bytes range (#950)
- refactor: Use simplified kv adapter instead (#959)

### Fixed

- fix(ops): Fix suffix range behavior of bytes range (#942)
- fix: Fix cache path not used correctly (#958)

## [v0.19.8] - 2022-11-13

### Added

- feat(services/moka): Use entry's bytes as capacity weigher (#914)
- feat: Implement rocksdb service (#913)

### Changed

- refactor: Reduce backend builder log level to debug (#907)
- refactor: Remove deprecated features (#920)
- refactor: use moka::sync::SegmentedCache (#921)

### Fixed

- fix(http): Check already read size before returning (#919)

## [v0.19.7] - 2022-10-31

### Added

- feat: Implement content type support for stat (#891)

### Changed

- refactor(layers/metrics): Holding all metrics handlers to avoid lock (#894)
- refactor(layers/metrics): Only update metrics while dropping readers (#896)

## [v0.19.6] - 2022-10-25

### Fixed

- fix: Metrics blocking reader doesn't handle operation correctly (#887)

## [v0.19.5] - 2022-10-24

### Added

- feat: add a feature named trust-dns (#879)
- feat: implement write_with (#880)
- feat: `content-type` configuration (#878)

### Fixed

- fix: Allow forward layers' acesser operations to inner (#884)

## [v0.19.4] - 2022-10-15

### Added

- feat: Improve into_stream by reduce zero byte fill (#864)
- debug: Add log for sync http client (#865)
- feat: Add debug log for finishing read (#867)
- feat: Try to use trust-dns-resolver (#869)
- feat: Add log for dropping reader and streamer (#870)

### Changed

- refactor: replace md5 with md-5 (#862)
- refactor: replace the hard code to X_AMZ_BUCKET_REGION constant (#866)

## [v0.19.3] - 2022-10-13

### Fixed

- fix: Retry for write is not implemented correctly (#860)

## [v0.19.2] - 2022-10-13

### Added

- feat(experiment): Allow user to config http connection pool (#843)
- feat: Add concurrent limit layer (#848)
- feat: Allow kv services implemented without list support (#850)
- feat: Implement service for moka (#852)
- docs: Add docs for moka service and concurrent limit layer (#857)

## [v0.19.1] - 2022-10-11

### Added

- feat: Allow retry read and write (#826)
- feat: Convert interrupted error to permanent after retry (#827)
- feat(services/ftp): Add connection pool for FTP (#832)
- feat: Implement retry for write operation (#831)
- feat: Bump reqsign to latest version (#837)
- feat(services/s3): Add role_arn and external_id for assume_role (#838)

### Changed

- test: accelerate behaviour test `test_list_rich_dir` (#828)

### Fixed

- fix: ObjectEntry returned in batch operator doesn't have correct accessor (#839)
- fix: Accessor in layers not set correctly (#840)

## [v0.19.0] - 2022-10-08

### Added

- feat: Implement object page stream for services like s3 (#787)
- RFC-0793: Generic KV Services (#793)
- feat(services/kv): Implement Scoped Key (#796)
- feat: Add scan in KeyValueAccessor (#797)
- feat: Implement basic kv services support (#799)
- feat: Introduce kv adapter for opendal (#802)
- feat: Add integration test for redis (#804)
- feat: Add OSS Service Support (#801)
- feat: Add integration tests for OSS (#814)

### Changed

- refactor: Move object to mod (#786)
- refactor: Implement azblob dir stream based on ObjectPageStream (#790)
- refactor: Implement memory services by generic kv (#800)
- refactor: Don't expose backend to users (#816)
- tests: allow running tests when env is `true` (#818)
- refactor: Remove deprecated type aliases (#819)
- test: list rich dir (#820)

### Fixed

- fix(services/redis): MATCH can't handle correctly (#803)
- fix: Disable ipfs redirection (#809)
- fix(services/ipfs): Use ipfs files API to copy data (#811)
- fix(services/hdfs): Allow retrying would block (#815)

## [v0.18.2] - 2022-10-01

### Added

- feat: Enable retry layer by default (#781)

### Changed

- ci: Enable IPFS NoFecth to avoid networking timeout (#780)
- ci: Build all feature in release to prevent build failure under release profile (#783)

### Fixed

- fix: Fix build error under release profile (#782)

## [v0.18.1] - 2022-10-01

### Fixed

- fix(services/s3): Content MD5 not set during list (#775)
- test: Add a test for ObjectEntry metadata cache (#776)

## [v0.18.0] - 2022-10-01

### Added

- feat: Add Metadata Cache Layer (#739)
- feat: Bump reqsign version to 0.5 (#741)
- feat: Derive Hash, Eq, PartialEq for Operation (#749)
- feat: Make AccessorMetadata public so outer users can use (#750)
- feat: Expose AccessorCapability to users (#751)
- feat: Expose opendal's http util to users (#753)
- feat: Implement convert from PresignedRequest (#756)
- feat: Make ObjectMetadata setter public (#758)
- feat: Implement cached metadata for ObjectEntry (#761)
- feat: Assign unique name for memory backend (#769)

### Changed

- refactor: replace error::other with new_other_object_error (#738)
- chore(compress): log with trace level instead of debug. (#752)
- refactor: Rename DirXxxx to ObjectXxxx instead (#759)

### Fixed

- fix(http_util): Disable auto compress and enable http proxy (#731)
- deps: Fix build after bump deps of oli and oay (#766)

## [v0.17.4] - 2022-09-27

### Fixed

- fix(http_util): Allow retry more errors (#724)
- fix(services/ftp): Suffix endpoints with default port (#726)

## [v0.17.3] - 2022-09-26

### Added

- feat: Add SubdirLayer to allowing switch directory (#718)
- feat(layers/retry): Add warning log while retry happened (#721)

### Fixed

- fix: update metrics on result (#716)
- fix: SubdirLayer should handle dir correctly (#720)

## [v0.17.2] - 2022-09-26

### Add

- feat: implement basic cp command (#688)
- chore: also parse 'FTPS' to Scheme::Ftp (#704)

### Changed

- refactor: remove `enable_secure` in FTP service (#709)
- oli: refactor copy implementation (#710)

### Fixed

- fix: Handle slash normalized false positives properly (#702)
- fix: Tracing is too verbose (#707)
- chore: fix error message in ftp service (#705)

## [v0.17.1] - 2022-09-19

### Added

- feat: redis service implement (#679)
- feat: Implement AsyncBufRead for IntoReader (#690)
- feat: expose security token of s3 (#693)

### Changed

- refactor: avoid unnecessary parent creating in Redis service (#692)
- refactor: Refactor HTTP Client to split sending and incoming logic (#695)

### Fixed

- fix: Handle write data in async way for IPMFS (#694)

## [v0.17.0] - 2022-09-15

### Added

- RFC: Path In Accessor (#661)
- feat: Implement RFC-0661: Path In Accessor (#664)
- feat: Hide http client internal details from users (#672)
- feat: make rustls the default tls implementation (#674)
- feat: Implement benches for layers (#681)

### Docs

- docs: Add how to implement service docs (#665)
- refactor: update redis support rfc (#676)
- docs: update metrics documentation (#684)

### Fixed

- fix: Immutable Index Layer could return duplicated paths (#671)
- fix: Remove not needed type parameter for immutable_layer (#677)
- fix: Don't trace buf field in poll_read (#682)
- fix: List non-exist dir should return empty (#683)
- fix: Add path validation for fs backend (#685)

## [v0.16.0] - 2022-09-12

### Added

- feat: Implement tests for read-only services (#634)
- feat(services/ftp): Implemented multi connection (#637)
- feat: Finalize FTP read operation (#644)
- feat: Implement service for IPFS HTTP Gateway (#645)
- feat: Add ImmutableIndexLayer (#651)
- feat: derive Hash for Scheme (#653)
- feat(services/ftp): Setup integration tests (#648)

### Changed

- refactor: Migrate all behavior tests to capability based (#635)
- refactor: Remove list support from http service (#639)
- refactor: Replace isahc with reqwest and ureq (#642)

### Deps

- deps: Bump reqsign to v0.4 (#643)
- deps: Remove not used features (#658)
- chore(deps): Update criterion requirement from 0.3 to 0.4 (#656)
- chore(deps): Update quick-xml requirement from 0.24 to 0.25 (#657)

### Docs

- docs: Add docs for ipfs (#649)
- docs: Fix typo (#650)
- docs: Add docs for ftp services (#655)

### RFCs

- RFC-0623: Redis Service (#623)

## [v0.15.0] - 2022-09-05

### Added

- RFC-0599: Blocking API (#599)
- feat: Add blocking API in Accessor (#604)
- feat: Implement blocking API for fs (#606)
- feat: improve observability of `BytesReader` and `DirStreamer` (#603)
- feat: Add behavior tests for blocking operations (#607)
- feat: Add integration tests for ipfs (#610)
- feat: implemented ftp backend (#581)
- RFC-0627: Split Capabilities (#627)

### Changed

- refactor: Extrace normalize_root functions (#619)
- refactor: Extrace build_abs_path and build_rooted_abs_path (#620)
- refactor: Extract build_rel_path (#621)
- feat: Rename ipfs to ipmfs to better reflect its naming (#629)

## [v0.14.1] - 2022-08-30

### Added

- feat: Add IPFS backend (#481)
- refactor: IPFS service cleanup (#590)

### Docs

- docs: Add obs in OpenDAL lib docs (#585)

### Fixed

- fix(services/s3): If input range is `0..`, don't insert range header (#592)

## [v0.14.0] - 2022-08-28

### Added

- RFC-0554: Write Refactor (#554)
- feat: Implement huaweicloud obs service other op support (#557)
- feat: Add new operations in Accessor (#564)
- feat: Implement obs create and write (#565)
- feat(services/s3): Implement Multipart support (#571)
- feat: Implement MultipartObject public API (#574)
- feat: Implement integration tests for multipart (#575)
- feat: Implement presign for write multipart (#576)
- test: Add assert of public struct size (#578)
- feat: List metadata reuse (#577)
- feat: Implement integration test for obs (#572)

### Changed

- refactor(ops): Promote ops as a parent mod (#553)
- refactor: Implement RFC-0554 Write Refactor (#556)
- refactor: Remove all unused qualifications (#560)
- refactor: Fix typo in azblob backend (#569)
- refactor: change ObjectError's op from &'static str to Operation (#580)

### Deleted

- refactor: Remove deprecated APIs (#582)

### Docs

- docs: Add docs for obs service (#579)

## [v0.13.1] - 2022-08-22

### Added

- feat: Add walk for BatchOperator (#543)
- feat: Mark Scheme non_exhaustive and extendable (#544)
- feat: Try to limit the max_connections for http client (#545)
- feat: Implement huaweicloud obs service read support (#540)

### Docs

- docs: Fix gcs is missing from index (#546)

## [v0.13.0] - 2022-08-17

### Added

- feat: Refactor metrics and hide under feature layers-metrics (#521)
- feat(layer): Add TracingLayer support (#523)
- feature: Google Cloud Storage support skeleton (#513)
- feat: Add LoggingLayer to replace service internal logs (#526)
- feat: Implement integration tests for gcs (#532)
- docs: Add docs for new layers (#534)
- docs: Add docs for gcs backend (#535)

### Changed

- refactor: Rewrite retry layer support (#522)

### Fixed

- fix: Make ProtocolViolation a retryable error (#528)

## [v0.12.0] - 2022-08-12

### Added

- RFC-0501: New Builder (#501)
- feat: Implement RFC-0501 New Builder (#510)

### Changed

- feat: Use isahc to replace hyper (#471)
- refactor: make parse http error code public (#511)
- refactor: Extrace new http error APIs (#515)
- refactor: Simplify the error response parse logic (#516)

### Removed

- refactor: Remove deprecated struct Metadata (#503)

## [v0.11.4] - 2022-08-02

### Added

- feat: Support using rustls for TLS (#491)

### Changed

- feat: try to support epoll (#478)
- deps: Lower the requirement of deps (#495)
- Revert "feat: try to support epoll" (#496)

### Fixed

- fix: Uri encode continuation-token before signing (#494)

### Docs

- docs: Add downloads in README (#485)
- docs: Update slogan for OpenDAL (#486)

## [v0.11.3] - 2022-07-26

### Changed

- build: Remove not used features (#472)

### Fixed

- fix: Disable connection pool as workaround for async runtime hang (#474)

### Dependencies

- chore(deps): Bump clap from 3.2.12 to 3.2.15 in /oay (#461)
- chore(deps): Bump clap from 3.2.12 to 3.2.15 in /oli (#460)
- chore(deps): Update metrics requirement from 0.19.0 to 0.20.0 (#462)
- chore(deps): Bump tokio from 1.20.0 to 1.20.1 in /oay (#468)

## [v0.11.2] - 2022-07-19

### Fixed

- fix: Service HTTP deosn't handle dir correctly (#455)
- fix: Service HTTP inserted with wrong key (#457)

## [v0.11.1] - 2022-07-19

### Added

- RFC-0438: Multipart (#438)
- RFC-0443: Gateway (#443)
- feat: Add basic oay support for http (#445)
- feat: BytesRange supports parsing from range and content-range (#449)
- feat(oay): Implement range support (#450)
- feat(services-http): Implement write and delete for testing (#451)

## [v0.11.0] - 2022-07-11

### Added

- feat: derive Deserialize/Serialize for ObjectMetaData (#420)
- RFC-0423: Command Line Interface (#423)
- feat: optimize range read (#425)
- feat(oli): Add basic layout for oli (#426)
- RFC-0429: Init From Iter (#429)
- feat: Implement RFC-0429 Init From Iter (#432)
- feat(oli): Add cp command layout (#428)

### Docs

- docs: Update description of OpenDAL (#434)

## [v0.10.0] - 2022-07-04

### Added

- RFC-0409: Accessor Capabilities (#409)
- feat: Implement RFC-0409 Accessor Capabilities (#411)
- RFC-0413: Presign (#413)
- feat: Implement presign support for s3 (#414)

### Docs

- docs: Add new RFCs in list (#415)

### Dependencies

- chore(deps): Update reqsign requirement from 0.1.1 to 0.2.0 (#412)

## [v0.9.1] - 2022-06-27

### Added

- feat(object): Add ETag support (#381)
- feat: Convert retryable hyper errors into Interrupted (#396)

### Changed

- build: Exclude docs from publish (#383)
- ci: Don't run CI on not needed push (#395)
- refactor: Use list for check instead of stat (#399)

### Dependencies

- chore(deps): Update size requirement from 0.1.2 to 0.2.0 (#385)
- Upgrade dev-dependency `size` to 0.4 (#392)

### Fixed

- fix: Special chars not handled correctly (#398)

## [v0.9.0] - 2022-06-14

### Added

- feat: Implement http service support (#368)
- feat: Add http_header to handle HTTP header parse (#369)
- feat(services/s3): Add virtual host API style support (#374)

### Changed

- refactor: Use the same http client across project (#364)
- refactor(services/{s3,azblob}): Make sure error response parsed correctly and safely (#375)

### Docs

- docs: Add concepts for Accessor, Operator and Object (#354)
- docs: Aad docs for batch operations (#363)

## [v0.8.0] - 2022-06-09

### Added

- RFC-0337: Dir Entry (#337)
- feat: Implement RFC-0337: Dir Entry (#342)
- feat: Add batch operation support (#346)

### Changed

- refactor: Rename Metadata to ObjectMetadata for clarify (#339)

### Others

- chore(deps): Bump actions/setup-python from 3 to 4 (#343)
- chore(deps): Bump amondnet/vercel-action from 20 to 25 (#344)

## [v0.7.3] - 2022-06-03

### Fixed

- fix(services/s3,hdfs): List empty dir should not return itself (#327)
- fix(services/hdfs): Root path not cleaned correctly (#330)

## [v0.7.2] - 2022-06-01

### Added

- feat(io_util): Improve debug logging for compress (#310)
- feat(services/s3): Add disable_credential_loader support (#317)
- feat: Allow check user input (#318)
- docs: Add services and features docs (#319)
- feat: Add name to object metadata (#304)
- fix(io_util/compress): Fix decoder's buf not all consumed (#323)

### Changed

- chore(deps): Update metrics requirement from 0.18.1 to 0.19.0 (#314)
- docs: Update README to reflect current status (#321)
- refactor(object): Make Metadata::name() return &str (#322)

### Fixed

- docs: Fix typo in examples (#320)
- fix(services): Don't throw error message for stat operation (#324)

## [v0.7.1] - 2022-05-29

### Fixed

- publish: Fix git version not allowed (#306)
- fix(io_util/compress): Decompress read exit too early (#308)

## [v0.7.0] - 2022-05-29

### Added

- feat: Add support for blocking decompress_read (#289)
- feat: Add check for operator (#290)
- docs: Use mdbook to generate documentation (#291)
- proposal: Object ID (#293)
- feat: Implement operator metadata support (#296)
- feat: Implement RFC-0293 Object ID (#298)

### Changed

- chore(deps): Update quick-xml requirement from 0.22.0 to 0.23.0 (#286)
- feat(io_util): Refactor decompress decoder (#302)
- ci: Adopt amondnet/vercel-action (#303)

### Fixed

- fix(services/aws): Increase retry times for AWS STS (#299)

## [v0.6.3] - 2022-05-25

### Added

- ci: Add all issues into databend-storage project (#277)
- feat(services/s3): Add retry in load_credential (#281)
- feat(services): Allow endpoint has trailing slash (#282)
- feat(services): Attach more context in error messages (#283)

## [v0.6.2] - 2022-05-12

### Fixed

- fix(azblob): Request URL not construct correctly (#270)

## [v0.6.1] - 2022-05-09

### Added

- feat: Add hdfs scheme (#266)

## [v0.6.0] - 2022-05-07

### Added

- docs: Improve docs to 100% coverage (#246)
- RFC-0247: Retryable Error (#247)
- feat: Implement retry layers (#249)
- feat: Implement retryable errors for azblob and s3 (#254)
- feat: Implement hdfs service support (#255)
- docs: Add docs for hdfs services (#262)

### Changed

- docs: Make sure code examples are formatted (#251)
- chore(deps): Update uuid requirement from 0.8.2 to 1.0.0 (#252)
- refactor: Remove deprecated modules (#259)

### Fixed

- ci: Fix docs build (#260)
- fix: HDFS jar not load (#261)

## [v0.5.2] - 2022-04-08

### Changed

- chore: Build all features for docs.rs (#238)
- ci: Enable auto dependence upgrade (#239)
- chore(deps): Bump actions/checkout from 2 to 3 (#240)
- docs: Refactor examples (#241)

### Fixed

- fix(services/s3): Endpoint without scheme should also supported (#242)

## [v0.5.1] - 2022-04-08

### Added

- docs: Add behavior docs for create operation (#235)

### Fixed

- fix(services/fs): Create on existing dir should succeed (#234)

## [v0.5.0] - 2022-04-07

### Added

- feat: Improve error message (#220)
- RFC-0221: Create Dir (#221)
- feat: Simplify create API (#225)
- feat: Implement decompress read support (#227)
- ci: Enable behavior test for azblob (#229)
- docs: Add docs for azblob's public structs (#230)

### Changed

- refactor: Move op.objects() to o.list() (#224)
- refactor: Improve behavior_tests so that cargo test works without --all-features (#231)

### Fixed

- fix: Azblob should pass all behavior tests now (#228)

## [v0.4.2] - 2022-04-03

### Added

- feat: Add seekable_reader on Object (#215)

### Fixed

- fix: Object last_modified should carry timezone (#217)

## [v0.4.1] - 2022-04-02

### Added

- feat: Export SeekableReader (#212)

## [v0.4.0] - 2022-04-02

**Refer to [Upgrade](./docs/upgrade.md) `From v0.3 to v0.4` section for more upgrade details.**

### Added

- feat(services/azblob): Implement list support (#193)
- feat: Implement io_util like into_sink and into_stream (#197)
- docs: Add docs for all newly added public functions (#199)
- feat(io_util): Implement observer for sink and stream (#198)
- docs: Add docs for public types (#206)

### Changed

- refactor: Make read return BytesStream instead (#192)
- RFC-0191: Async Streaming IO (#191)
- refactor: New public API design (#201)
- refactor: Adopt io::Result instead (#204)
- refactor: Rollback changes around async streaming io (#205)
- refactor: Refactor behavior tests with macro_rules (#207)

### Fixed

- deps: Bump to reqsign to fix s3 url encode issue (#202)

### Removed

- RFC-0203: Remove Credential (#203)

## [v0.3.0] - 2022-03-25

### Added

- feat: Add azure blob support (#165)
- feat: Add tracing support via minitrace (#175)
- feat(service/s3): Implement server side encryption support (#182)

### Changed

- chore: Level down some log entry to debug (#181)

### Fixed

- fix(service/s3): Endpoint template should be applied if region exists (#180)

## [v0.2.5] - 2022-03-22

### Added

- feat: Adopt quick_xml to parse xml (#164)
- test: Add behavior test for not exist object (#166)
- feat: Allow user input region (#168)

### Changed

- feat: Improve error handle for s3 service (#169)
- feat: Read error response for better debugging (#170)
- examples: Improve examples for s3 (#171)

## [v0.2.4] - 2022-03-18

### Added

- feat: Add content_md5 and last_modified in metadata (#158)

### Changed

- refactor: Say goodbye to aws-s3-sdk (#152)

## [v0.2.3] - 2022-03-14

### Added

- feat: Export BoxedObjectStream so that users can implement Layer (#147)

## [v0.2.2] - 2022-03-14

### Fixed

- services/fs: Refactor via tokio::fs (#142)
- fix: Stat root should return a dir object (#143)

## [v0.2.1] - 2022-03-10

### Added

- \*: Implement logging support (#122)
- feat(service): Add service memory read support (#121)
- services: Add basic metrics (#127)
- services: Add full memory support (#134)

### Changed

- benches: Refactor to support more read pattern (#126)
- services: Refactor into directories (#131)

### Docs

- docs: Cover all public types and functions (#128)
- docs: Update README (#129)
- ci: Generate main docs to <opendal.apache.org> (#132)
- docs: Enrich README (#133)
- Add examples for object (#135)

## [v0.2.0] - 2022-03-08

### Added

- RFC-112: Path Normalization (#112)
- examples: Add more examples for services and operations (#113)

### Changed

- benches: Refactor to make code more readable (#104)
- object: Refactor ObjectMode into enum (#114)

## [v0.1.4] - 2022-03-04

### Added

- services/s3: Implement anonymous read support (#97)
- bench: Add parallel_read bench (#100)
- services/s3: Add test for anonymous support (#99)

## [v0.1.3] - 2022-03-02

### Added

- RFC and implementations for limited reader (#90)
- readers: Implement observe reader support (#92)

### Changed

- deps: Bump s3 sdk to 0.8 (#87)
- bench: Improve logic (#89)

### New RFCs

- [limited_reader](https://github.com/apache/opendal/blob/main/docs/rfcs/0090-limited-reader.md)

## [v0.1.2] - 2022-03-01

### Changed

- object: Polish API for Metadata (#80)

## [v0.1.1] - 2022-03-01

### Added

- RFC and implementation of feature Object Stream (#69)
- services/s3: Implement List support (#76)
- credential: Add Plain variant to allow more input (#78)

### Changed

- backend/s3: Change from lazy_static to once_cell (#62)
- backend/s3: Enable test on AWS S3 (#64)

## [v0.1.0] - 2022-02-24

### Added

- docs: Add README for behavior test and ops benchmarks (#53)
- RFC-0057: Auto Region (#57)
- backend/s3: Implement RFC-57 Auto Region (#59)

### Changed

- io: Rename BoxedAsyncRead to BoxedAsyncReader (#55)
- \*: Refactor tests (#60)

## [v0.0.5] - 2022-02-23

### Fixed

- io: Remove not used debug print (#48)

## [v0.0.4] - 2022-02-23

### Added

- readers: Allow config prefetch size (#31)
- RFC-0041: Object Native API (#41)
- \*: Implement RFC-0041 Object Native API (#35)
- RFC-0044: Error Handle (#44)
- error: Implement RFC-0044 Error Handle (#43)

### Changed

- services/fs: Use separate dedicated thread pool instead (#42)

## [v0.0.3] - 2022-02-16

### Added

- benches: Implement benches for ops (#26)

### Changed

- services/s3: Don't load_from_env if users already inputs (#23)
- readers: Improve seekable performance (#25)

## [v0.0.2] - 2022-02-15

### Added

- tests: Implement behavior tests (#13)
- services/s3: Add support for endpoints without scheme (#15)
- tests: Implement integration tests for s3 (#18)

### Fixed

- services/s3: Allow set endpoint and region while input value is valid (#17)

## v0.0.1 - 2022-02-14

### Added

Hello, OpenDAL!

[v0.52.0]: https://github.com/apache/opendal/compare/v0.51.2...v0.52.0
[v0.51.2]: https://github.com/apache/opendal/compare/v0.51.1...v0.51.2
[v0.51.1]: https://github.com/apache/opendal/compare/v0.51.0...v0.51.1
[v0.51.0]: https://github.com/apache/opendal/compare/v0.50.2...v0.51.0
[v0.50.2]: https://github.com/apache/opendal/compare/v0.50.1...v0.50.2
[v0.50.1]: https://github.com/apache/opendal/compare/v0.50.0...v0.50.1
[v0.50.0]: https://github.com/apache/opendal/compare/v0.49.2...v0.50.0
[v0.49.2]: https://github.com/apache/opendal/compare/v0.49.1...v0.49.2
[v0.49.1]: https://github.com/apache/opendal/compare/v0.49.0...v0.49.1
[v0.49.0]: https://github.com/apache/opendal/compare/v0.48.0...v0.49.0
[v0.48.0]: https://github.com/apache/opendal/compare/v0.47.3...v0.48.0
[v0.47.3]: https://github.com/apache/opendal/compare/v0.47.2...v0.47.3
[v0.47.2]: https://github.com/apache/opendal/compare/v0.47.1...v0.47.2
[v0.47.1]: https://github.com/apache/opendal/compare/v0.47.0...v0.47.1
[v0.47.0]: https://github.com/apache/opendal/compare/v0.46.0...v0.47.0
[v0.46.0]: https://github.com/apache/opendal/compare/v0.45.1...v0.46.0
[v0.45.1]: https://github.com/apache/opendal/compare/v0.45.0...v0.45.1
[v0.45.0]: https://github.com/apache/opendal/compare/v0.44.2...v0.45.0
[v0.44.2]: https://github.com/apache/opendal/compare/v0.44.1...v0.44.2
[v0.44.1]: https://github.com/apache/opendal/compare/v0.44.0...v0.44.1
[v0.44.0]: https://github.com/apache/opendal/compare/v0.43.0...v0.44.0
[v0.43.0]: https://github.com/apache/opendal/compare/v0.42.0...v0.43.0
[v0.42.0]: https://github.com/apache/opendal/compare/v0.41.0...v0.42.0
[v0.41.0]: https://github.com/apache/opendal/compare/v0.40.0...v0.41.0
[v0.40.0]: https://github.com/apache/opendal/compare/v0.39.1...v0.40.0
[v0.39.0]: https://github.com/apache/opendal/compare/v0.38.1...v0.39.0
[v0.38.1]: https://github.com/apache/opendal/compare/v0.38.0...v0.38.1
[v0.38.0]: https://github.com/apache/opendal/compare/v0.37.0...v0.38.0
[v0.37.0]: https://github.com/apache/opendal/compare/v0.36.0...v0.37.0
[v0.36.0]: https://github.com/apache/opendal/compare/v0.35.0...v0.36.0
[v0.35.0]: https://github.com/apache/opendal/compare/v0.34.0...v0.35.0
[v0.34.0]: https://github.com/apache/opendal/compare/v0.33.3...v0.34.0
[v0.33.3]: https://github.com/apache/opendal/compare/v0.33.2...v0.33.3
[v0.33.2]: https://github.com/apache/opendal/compare/v0.33.1...v0.33.2
[v0.33.1]: https://github.com/apache/opendal/compare/v0.33.0...v0.33.1
[v0.33.0]: https://github.com/apache/opendal/compare/v0.32.0...v0.33.0
[v0.32.0]: https://github.com/apache/opendal/compare/v0.31.1...v0.32.0
[v0.31.1]: https://github.com/apache/opendal/compare/v0.31.0...v0.31.1
[v0.31.0]: https://github.com/apache/opendal/compare/v0.30.5...v0.31.0
[v0.30.5]: https://github.com/apache/opendal/compare/v0.30.4...v0.30.5
[v0.30.4]: https://github.com/apache/opendal/compare/v0.30.3...v0.30.4
[v0.30.3]: https://github.com/apache/opendal/compare/v0.30.2...v0.30.3
[v0.30.2]: https://github.com/apache/opendal/compare/v0.30.1...v0.30.2
[v0.30.1]: https://github.com/apache/opendal/compare/v0.30.0...v0.30.1
[v0.30.0]: https://github.com/apache/opendal/compare/v0.29.1...v0.30.0
[v0.29.1]: https://github.com/apache/opendal/compare/v0.29.0...v0.29.1
[v0.29.0]: https://github.com/apache/opendal/compare/v0.28.0...v0.29.0
[v0.28.0]: https://github.com/apache/opendal/compare/v0.27.2...v0.28.0
[v0.27.2]: https://github.com/apache/opendal/compare/v0.27.1...v0.27.2
[v0.27.1]: https://github.com/apache/opendal/compare/v0.27.0...v0.27.1
[v0.27.0]: https://github.com/apache/opendal/compare/v0.26.2...v0.27.0
[v0.26.2]: https://github.com/apache/opendal/compare/v0.26.1...v0.26.2
[v0.26.1]: https://github.com/apache/opendal/compare/v0.26.0...v0.26.1
[v0.26.0]: https://github.com/apache/opendal/compare/v0.25.2...v0.26.0
[v0.25.2]: https://github.com/apache/opendal/compare/v0.25.1...v0.25.2
[v0.25.1]: https://github.com/apache/opendal/compare/v0.25.0...v0.25.1
[v0.25.0]: https://github.com/apache/opendal/compare/v0.24.6...v0.25.0
[v0.24.6]: https://github.com/apache/opendal/compare/v0.24.5...v0.24.6
[v0.24.5]: https://github.com/apache/opendal/compare/v0.24.4...v0.24.5
[v0.24.4]: https://github.com/apache/opendal/compare/v0.24.3...v0.24.4
[v0.24.3]: https://github.com/apache/opendal/compare/v0.24.2...v0.24.3
[v0.24.2]: https://github.com/apache/opendal/compare/v0.24.1...v0.24.2
[v0.24.1]: https://github.com/apache/opendal/compare/v0.24.0...v0.24.1
[v0.24.0]: https://github.com/apache/opendal/compare/v0.23.0...v0.24.0
[v0.23.0]: https://github.com/apache/opendal/compare/v0.22.6...v0.23.0
[v0.22.6]: https://github.com/apache/opendal/compare/v0.22.5...v0.22.6
[v0.22.5]: https://github.com/apache/opendal/compare/v0.22.4...v0.22.5
[v0.22.4]: https://github.com/apache/opendal/compare/v0.22.3...v0.22.4
[v0.22.3]: https://github.com/apache/opendal/compare/v0.22.2...v0.22.3
[v0.22.2]: https://github.com/apache/opendal/compare/v0.22.1...v0.22.2
[v0.22.1]: https://github.com/apache/opendal/compare/v0.22.0...v0.22.1
[v0.22.0]: https://github.com/apache/opendal/compare/v0.21.2...v0.22.0
[v0.21.2]: https://github.com/apache/opendal/compare/v0.21.1...v0.21.2
[v0.21.1]: https://github.com/apache/opendal/compare/v0.21.0...v0.21.1
[v0.21.0]: https://github.com/apache/opendal/compare/v0.20.1...v0.21.0
[v0.20.1]: https://github.com/apache/opendal/compare/v0.20.0...v0.20.1
[v0.20.0]: https://github.com/apache/opendal/compare/v0.19.8...v0.20.0
[v0.19.8]: https://github.com/apache/opendal/compare/v0.19.7...v0.19.8
[v0.19.7]: https://github.com/apache/opendal/compare/v0.19.6...v0.19.7
[v0.19.6]: https://github.com/apache/opendal/compare/v0.19.5...v0.19.6
[v0.19.5]: https://github.com/apache/opendal/compare/v0.19.4...v0.19.5
[v0.19.4]: https://github.com/apache/opendal/compare/v0.19.3...v0.19.4
[v0.19.3]: https://github.com/apache/opendal/compare/v0.19.2...v0.19.3
[v0.19.2]: https://github.com/apache/opendal/compare/v0.19.1...v0.19.2
[v0.19.1]: https://github.com/apache/opendal/compare/v0.19.0...v0.19.1
[v0.19.0]: https://github.com/apache/opendal/compare/v0.18.2...v0.19.0
[v0.18.2]: https://github.com/apache/opendal/compare/v0.18.1...v0.18.2
[v0.18.1]: https://github.com/apache/opendal/compare/v0.18.0...v0.18.1
[v0.18.0]: https://github.com/apache/opendal/compare/v0.17.4...v0.18.0
[v0.17.4]: https://github.com/apache/opendal/compare/v0.17.3...v0.17.4
[v0.17.3]: https://github.com/apache/opendal/compare/v0.17.2...v0.17.3
[v0.17.2]: https://github.com/apache/opendal/compare/v0.17.1...v0.17.2
[v0.17.1]: https://github.com/apache/opendal/compare/v0.17.0...v0.17.1
[v0.17.0]: https://github.com/apache/opendal/compare/v0.16.0...v0.17.0
[v0.16.0]: https://github.com/apache/opendal/compare/v0.15.0...v0.16.0
[v0.15.0]: https://github.com/apache/opendal/compare/v0.14.1...v0.15.0
[v0.14.1]: https://github.com/apache/opendal/compare/v0.14.0...v0.14.1
[v0.14.0]: https://github.com/apache/opendal/compare/v0.13.1...v0.14.0
[v0.13.1]: https://github.com/apache/opendal/compare/v0.13.0...v0.13.1
[v0.13.0]: https://github.com/apache/opendal/compare/v0.12.0...v0.13.0
[v0.12.0]: https://github.com/apache/opendal/compare/v0.11.4...v0.12.0
[v0.11.4]: https://github.com/apache/opendal/compare/v0.11.3...v0.11.4
[v0.11.3]: https://github.com/apache/opendal/compare/v0.11.2...v0.11.3
[v0.11.2]: https://github.com/apache/opendal/compare/v0.11.1...v0.11.2
[v0.11.1]: https://github.com/apache/opendal/compare/v0.11.0...v0.11.1
[v0.11.0]: https://github.com/apache/opendal/compare/v0.10.0...v0.11.0
[v0.10.0]: https://github.com/apache/opendal/compare/v0.9.1...v0.10.0
[v0.9.1]: https://github.com/apache/opendal/compare/v0.9.0...v0.9.1
[v0.9.0]: https://github.com/apache/opendal/compare/v0.8.0...v0.9.0
[v0.8.0]: https://github.com/apache/opendal/compare/v0.7.3...v0.8.0
[v0.7.3]: https://github.com/apache/opendal/compare/v0.7.2...v0.7.3
[v0.7.2]: https://github.com/apache/opendal/compare/v0.7.1...v0.7.2
[v0.7.1]: https://github.com/apache/opendal/compare/v0.7.0...v0.7.1
[v0.7.0]: https://github.com/apache/opendal/compare/v0.6.3...v0.7.0
[v0.6.3]: https://github.com/apache/opendal/compare/v0.6.2...v0.6.3
[v0.6.2]: https://github.com/apache/opendal/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com/apache/opendal/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com/apache/opendal/compare/v0.5.2...v0.6.0
[v0.5.2]: https://github.com/apache/opendal/compare/v0.5.1...v0.5.2
[v0.5.1]: https://github.com/apache/opendal/compare/v0.5.0...v0.5.1
[v0.5.0]: https://github.com/apache/opendal/compare/v0.4.2...v0.5.0
[v0.4.2]: https://github.com/apache/opendal/compare/v0.4.1...v0.4.2
[v0.4.1]: https://github.com/apache/opendal/compare/v0.4.0...v0.4.1
[v0.4.0]: https://github.com/apache/opendal/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/apache/opendal/compare/v0.2.5...v0.3.0
[v0.2.5]: https://github.com/apache/opendal/compare/v0.2.4...v0.2.5
[v0.2.4]: https://github.com/apache/opendal/compare/v0.2.3...v0.2.4
[v0.2.3]: https://github.com/apache/opendal/compare/v0.2.2...v0.2.3
[v0.2.2]: https://github.com/apache/opendal/compare/v0.2.1...v0.2.2
[v0.2.1]: https://github.com/apache/opendal/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/apache/opendal/compare/v0.1.4...v0.2.0
[v0.1.4]: https://github.com/apache/opendal/compare/v0.1.3...v0.1.4
[v0.1.3]: https://github.com/apache/opendal/compare/v0.1.2...v0.1.3
[v0.1.2]: https://github.com/apache/opendal/compare/v0.1.1...v0.1.2
[v0.1.1]: https://github.com/apache/opendal/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/apache/opendal/compare/v0.0.5...v0.1.0
[v0.0.5]: https://github.com/apache/opendal/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/apache/opendal/compare/v0.0.3...v0.0.4
[v0.0.3]: https://github.com/apache/opendal/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/apache/opendal/compare/v0.0.1...v0.0.2
