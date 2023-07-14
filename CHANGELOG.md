# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v0.38.1] - 2023-07-14

### Added

- feat(binding/lua): add rename and create_dir operator function by @oowl in https://github.com/apache/incubator-opendal/pull/2564
- feat(services/azblob): support sink by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2574
- feat(services/gcs): support sink by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2576
- feat(services/oss): support sink by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2577
- feat(services/obs): support sink by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2578
- feat(services/cos): impl sink by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2587
- feat(service): Support stat for Dropbox by @Zheaoli in https://github.com/apache/incubator-opendal/pull/2588
- feat(services/dropbox): impl create_dir and polish error handling by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2600
- feat(services/dropbox): Implement refresh token support by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2604
- feat(service/dropbox): impl batch delete by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2606
- feat(CI): set Kvrocks test for service redis by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2613
- feat(core): object versioning APIs by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2614
- feat(oay): actually read configuration from `oay.toml` by @messense in https://github.com/apache/incubator-opendal/pull/2615
- feat(services/webdav): impl sink by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2622
- feat(services/fs): impl Sink for Fs by @Ji-Xinyou in https://github.com/apache/incubator-opendal/pull/2626
- feat(core): impl `delete_with` on blocking operator by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2633
- feat(bindings/C): add support for list in C binding by @Ji-Xinyou in https://github.com/apache/incubator-opendal/pull/2448
- feat(services/s3): Add detect_region support for S3Builder by @parkma99 in https://github.com/apache/incubator-opendal/pull/2634

### Changed

- refactor(core): Add ErrorKind InvalidInput to indicate users input error by @dqhl76 in https://github.com/apache/incubator-opendal/pull/2637
- refactor(services/s3): Add more detect logic for detect_region by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2645

### Fixed

- fix(doc): fix codeblock rendering by @xxchan in https://github.com/apache/incubator-opendal/pull/2592
- fix(service/minitrace): should set local parent by @andylokandy in https://github.com/apache/incubator-opendal/pull/2620
- fix(service/minitrace): update doc by @andylokandy in https://github.com/apache/incubator-opendal/pull/2621

### Docs

- doc(bindings/haskell): add module document by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2566
- docs: Update license related comments by @Prashanth-Chandra in https://github.com/apache/incubator-opendal/pull/2573
- docs: add hdfs namenode High Availability related troubleshoot by @wcy-fdu in https://github.com/apache/incubator-opendal/pull/2601
- docs: polish release doc by @PsiACE in https://github.com/apache/incubator-opendal/pull/2608
- docs(blog): add Apache OpenDAL(Incubating): Access Data Freely by @PsiACE in https://github.com/apache/incubator-opendal/pull/2607
- docs(RFC): Object Versioning by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2602

### CI

- ci: Disable bindings/java deploy for now by @tisonkun in https://github.com/apache/incubator-opendal/pull/2560
- ci: Disable the failed stage-release job instead by @tisonkun in https://github.com/apache/incubator-opendal/pull/2561
- ci: add haddock generator for haskell binding by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2569
- ci(binding/lua): add luarocks package manager support by @oowl in https://github.com/apache/incubator-opendal/pull/2558
- build(deps): bump predicates from 2.1.5 to 3.0.1 by @dependabot in https://github.com/apache/incubator-opendal/pull/2583
- build(deps): bump tower-http from 0.4.0 to 0.4.1 by @dependabot in https://github.com/apache/incubator-opendal/pull/2582
- build(deps): bump chrono from 0.4.24 to 0.4.26 by @dependabot in https://github.com/apache/incubator-opendal/pull/2581
- build(deps): bump redis from 0.22.3 to 0.23.0 by @dependabot in https://github.com/apache/incubator-opendal/pull/2580
- build(deps): bump cbindgen from 0.24.3 to 0.24.5 by @dependabot in https://github.com/apache/incubator-opendal/pull/2579
- ci: upgrade hawkeye to v3 by @tisonkun in https://github.com/apache/incubator-opendal/pull/2585
- ci(services/webdav): Setup integration test for nextcloud by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2631

### Chore

- chore: add haskell binding link to website by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2571
- chore: fix cargo warning for resolver by @xxchan in https://github.com/apache/incubator-opendal/pull/2590
- chore: bump log to 0.4.19 by @xxchan in https://github.com/apache/incubator-opendal/pull/2591
- chore(deps): update deps to latest version by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2596
- chore: Add release 0.38.0 to download by @PsiACE in https://github.com/apache/incubator-opendal/pull/2597
- chore(service/minitrace): automatically generate span name by @andylokandy in https://github.com/apache/incubator-opendal/pull/2618

## New Contributors

- @Prashanth-Chandra made their first contribution in https://github.com/apache/incubator-opendal/pull/2573
- @andylokandy made their first contribution in https://github.com/apache/incubator-opendal/pull/2618
- @parkma99 made their first contribution in https://github.com/apache/incubator-opendal/pull/2634

**Full Changelog**: https://github.com/apache/incubator-opendal/compare/v0.38.0...v0.38.1

## [v0.38.0] - 2023-06-27

### Added

- feat(raw/http_util): Implement mixed multipart parser by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2430
- feat(services/gcs): Add batch delete support by @wcy-fdu in https://github.com/apache/incubator-opendal/pull/2142
- feat(core): Add Write::sink API by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2440
- feat(services/s3): Allow retry for unexpected 499 error by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2453
- feat(layer): add throttle layer by @morristai in https://github.com/apache/incubator-opendal/pull/2444
- feat(bindings/haskell): init haskell binding by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2463
- feat(core): add capability check by @unixzii in https://github.com/apache/incubator-opendal/pull/2461
- feat(bindings/haskell): add CONTRIBUTING.md by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2466
- feat(bindings/haskell): add CI test for haskell binding by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2468
- feat(binding/lua): introduce opendal lua binding by @oowl in https://github.com/apache/incubator-opendal/pull/2469
- feat(bindings/swift): add Swift binding by @unixzii in https://github.com/apache/incubator-opendal/pull/2470
- feat(bindings/haskell): support `is_exist` `create_dir` `copy` `rename` `delete` by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2475
- feat(bindings/haskell): add `Monad` wrapper by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2482
- feat(bindings/dotnet): basic structure by @tisonkun in https://github.com/apache/incubator-opendal/pull/2485
- feat(services/dropbox): Support create/read/delete for Dropbox by @Zheaoli in https://github.com/apache/incubator-opendal/pull/2264
- feat(bindings/java): support load system lib by @tisonkun in https://github.com/apache/incubator-opendal/pull/2502
- feat(blocking operator): add remove_all api by @infdahai in https://github.com/apache/incubator-opendal/pull/2449
- feat(core): adopt WebHDFS LISTSTATUS_BATCH for better performance by @morristai in https://github.com/apache/incubator-opendal/pull/2499
- feat(bindings/haskell): support stat by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2504
- feat(adapters-kv): add rename and copy support to kv adapters by @oowl in https://github.com/apache/incubator-opendal/pull/2513
- feat: Implement sink for services s3 by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2508
- feat(adapters-kv): add rename and copy support to non typed kv adapters by @oowl in https://github.com/apache/incubator-opendal/pull/2515
- feat: Implement test harness via libtest-mimic instead by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2517
- feat(service/sled): introduce tree support by @oowl in https://github.com/apache/incubator-opendal/pull/2516
- feat(bindings/haskell): support list and scan by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2527
- feat(services/redb): support redb service by @oowl in https://github.com/apache/incubator-opendal/pull/2526
- feat(core): implement service for Mini Moka by @morristai in https://github.com/apache/incubator-opendal/pull/2537
- feat(core): add Mini Moka GitHub Action workflow job by @morristai in https://github.com/apache/incubator-opendal/pull/2539
- feat(services): add cacache backend by @PsiACE in https://github.com/apache/incubator-opendal/pull/2548
- feat: Implement Writer::copy so user can copy from AsyncRead by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2552

### Changed

- refactor(bindings/C): refactor c bindings to call all APIs using pointer by @Ji-Xinyou in https://github.com/apache/incubator-opendal/pull/2489

### Fixed

- fix(services/azblob): Fix azblob batch max operations by @A-Stupid-Sun in https://github.com/apache/incubator-opendal/pull/2434
- fix(services/sftp): change default root config to remote server setting by @silver-ymz in https://github.com/apache/incubator-opendal/pull/2431
- fix: Enable `std` feature for futures to allow `futures::AsyncRead` by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2450
- fix(services/gcs): GCS should support create dir by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2467
- fix(bindings/C): use copy_from_slice instead of from_static in opendal_bytes by @Ji-Xinyou in https://github.com/apache/incubator-opendal/pull/2473
- fix(bindings/swift): reorg the package to correct its name by @unixzii in https://github.com/apache/incubator-opendal/pull/2479
- fix: Fix the build for zig binding by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2493
- fix(service/webhdfs): fix webhdfs config builder for disable_list_batch by @morristai in https://github.com/apache/incubator-opendal/pull/2509
- fix(core/types): add missing `vercel artifacts` for `FromStr` by @cijiugechu in https://github.com/apache/incubator-opendal/pull/2519
- fix(types/operator): fix operation limit error default size by @oowl in https://github.com/apache/incubator-opendal/pull/2536

### Docs

- docs: Replace `create` with `new` by @NiwakaDev in https://github.com/apache/incubator-opendal/pull/2427
- docs(services/redis): fix redis via config example by @A-Stupid-Sun in https://github.com/apache/incubator-opendal/pull/2443
- docs: add rust usage example by @Young-Flash in https://github.com/apache/incubator-opendal/pull/2447
- docs: Polish rust examples by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2456
- docs: polish docs and fix typos by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2458
- docs: fix a typo on the landing page by @unixzii in https://github.com/apache/incubator-opendal/pull/2460
- docs(examples/rust): Add 01-init-operator by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2464
- docs: update readme.md to match the output by @rrain7 in https://github.com/apache/incubator-opendal/pull/2486
- docs: Update components for Libraries and Services by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2487
- docs: Add OctoBase into our users list by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2506
- docs: Fix scan not checked for sled services by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2507
- doc(binding/lua): Improve readme doc for contribute and usage by @oowl in https://github.com/apache/incubator-opendal/pull/2511
- doc(services/redb): add doc for redb service backend by @oowl in https://github.com/apache/incubator-opendal/pull/2538
- doc(bindings/swift): add CONTRIBUTING.md by @unixzii in https://github.com/apache/incubator-opendal/pull/2540
- docs: Add new rust example 02-async-io by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2541
- docs: Fix link for CONTRIBUTING.md by @HuSharp in https://github.com/apache/incubator-opendal/pull/2544
- doc: polish release doc by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2531
- docs: Move verify to upper folder by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2546
- doc(binding/lua): add ldoc generactor for lua binding by @oowl in https://github.com/apache/incubator-opendal/pull/2549
- docs: Add new architectural image for OpenDAL by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2553
- docs: Polish README for core and bindings by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2554

### CI

- ci: Fix append test should use copy_buf to avoid call times by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2436
- build(bindings/ruby): fix compile rb-sys on Apple M1 by @tisonkun in https://github.com/apache/incubator-opendal/pull/2451
- ci: Use summary for zig test to fix build by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2480
- ci(workflow): add lua binding test workflow by @oowl in https://github.com/apache/incubator-opendal/pull/2478
- build(deps): bump actions/setup-python from 3 to 4 by @dependabot in https://github.com/apache/incubator-opendal/pull/2481
- ci(bindings/swift): add CI for Swift binding by @unixzii in https://github.com/apache/incubator-opendal/pull/2492
- ci: Try to make webhdfs tests more stable by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2503
- ci(bindings/java): auto release snapshot by @tisonkun in https://github.com/apache/incubator-opendal/pull/2521
- ci: Disable the stage snapshot CI by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2528
- ci: fix opendal-java snapshot releases by @tisonkun in https://github.com/apache/incubator-opendal/pull/2532
- ci: Fix typo in binding java CI by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2534
- ci(bindings/swift): optimize time consumption of CI pipeline by @unixzii in https://github.com/apache/incubator-opendal/pull/2545
- ci: Fix PR label not updated while edited by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2547

### Chore

- chore: Add redis bench support by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2438
- chore(bindings/nodejs): update index.d.ts by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2459
- chore: Add release 0.37.0 to download by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2472
- chore: Fix Cargo.lock not updated by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2490
- chore: Polish some code details by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2505
- chore(bindings/nodejs): provide more precise type for scheme by @cijiugechu in https://github.com/apache/incubator-opendal/pull/2520

## [v0.37.0] - 2023-06-06

### Added

- feat(services/webdav): support redirection when get 302/307 response during read operation by @Yansongsongsong in https://github.com/apache/incubator-opendal/pull/2256
- feat: Add Zig Bindings Module by @kassane in https://github.com/apache/incubator-opendal/pull/2374
- feat: Implement Timeout Layer by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2395
- feat(bindings/c): add opendal_operator_blocking_delete method by @jiaoew1991 in https://github.com/apache/incubator-opendal/pull/2416
- feat(services/obs): add append support by @infdahai in https://github.com/apache/incubator-opendal/pull/2422

### Changed

- refactor(bindings/zig): enable tests and more by @tisonkun in https://github.com/apache/incubator-opendal/pull/2375
- refactor(bindings/zig): add errors handler and module test by @kassane in https://github.com/apache/incubator-opendal/pull/2381
- refactor(http_util): Adopt reqwest's redirect support by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2390

### Fixed

- fix(bindings/zig): reflect C interface changes by @tisonkun in https://github.com/apache/incubator-opendal/pull/2378
- fix(services/azblob): Fix batch delete doesn't work on azure by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2382
- fix(services/oss): Fix oss batch max operations by @A-Stupid-Sun in https://github.com/apache/incubator-opendal/pull/2414
- fix(core): Don't wake up operator futures while not ready by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2415
- fix(services/s3): Fix s3 batch max operations by @A-Stupid-Sun in https://github.com/apache/incubator-opendal/pull/2418

### Docs

- docs: service doc for s3 by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2376
- docs(bindings/C): The documentation for OpenDAL C binding by @Ji-Xinyou in https://github.com/apache/incubator-opendal/pull/2373
- docs: add link for c binding by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2380
- docs: docs for kv services by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2396
- docs: docs for fs related services by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2397
- docs(bindings/java): do not release snapshot versions anymore by @tisonkun in https://github.com/apache/incubator-opendal/pull/2398
- docs: doc for ipmfs by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2408
- docs: add service doc for oss by @A-Stupid-Sun in https://github.com/apache/incubator-opendal/pull/2409
- docs: improvement of Python binding by @ideal in https://github.com/apache/incubator-opendal/pull/2411
- docs: doc for download by @suyanhanx in https://github.com/apache/incubator-opendal/pull/2424
- docs: Add release guide by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2425

### CI

- ci: Enable semantic PRs by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2370
- ci: improve licenserc settings by @tisonkun in https://github.com/apache/incubator-opendal/pull/2377
- build(deps): bump reqwest from 0.11.15 to 0.11.18 by @dependabot in https://github.com/apache/incubator-opendal/pull/2389
- build(deps): bump pyo3 from 0.18.2 to 0.18.3 by @dependabot in https://github.com/apache/incubator-opendal/pull/2388
- ci: Enable nextest for all behavior tests by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2400
- ci: reflect ascii file rewrite by @tisonkun in https://github.com/apache/incubator-opendal/pull/2419
- ci: Remove website from git archive by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2420
- ci: Add integration tests for Cloudflare R2 by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2423

### Chore

- chore(bindings/python): upgrade maturin to 1.0 by @messense in https://github.com/apache/incubator-opendal/pull/2369
- chore: Fix license headers for release/labler by @Xuanwo in https://github.com/apache/incubator-opendal/pull/2371
- chore(bindings/C): add one simple read/write example into readme and code by @Ji-Xinyou in https://github.com/apache/incubator-opendal/pull/2421

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
- chore: replace datafuselabs/opendal with apache/incubator-opendal (#1647)
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

- [limited_reader](https://github.com/apache/incubator-opendal/blob/main/docs/rfcs/0090-limited-reader.md)

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

[v0.38.1]: https://github.com/apache/incubator-opendal/compare/v0.38.0...v0.38.1
[v0.38.0]: https://github.com/apache/incubator-opendal/compare/v0.37.0...v0.38.0
[v0.37.0]: https://github.com/apache/incubator-opendal/compare/v0.36.0...v0.37.0
[v0.36.0]: https://github.com/apache/incubator-opendal/compare/v0.35.0...v0.36.0
[v0.35.0]: https://github.com/apache/incubator-opendal/compare/v0.34.0...v0.35.0
[v0.34.0]: https://github.com/apache/incubator-opendal/compare/v0.33.3...v0.34.0
[v0.33.3]: https://github.com/apache/incubator-opendal/compare/v0.33.2...v0.33.3
[v0.33.2]: https://github.com/apache/incubator-opendal/compare/v0.33.1...v0.33.2
[v0.33.1]: https://github.com/apache/incubator-opendal/compare/v0.33.0...v0.33.1
[v0.33.0]: https://github.com/apache/incubator-opendal/compare/v0.32.0...v0.33.0
[v0.32.0]: https://github.com/apache/incubator-opendal/compare/v0.31.1...v0.32.0
[v0.31.1]: https://github.com/apache/incubator-opendal/compare/v0.31.0...v0.31.1
[v0.31.0]: https://github.com/apache/incubator-opendal/compare/v0.30.5...v0.31.0
[v0.30.5]: https://github.com/apache/incubator-opendal/compare/v0.30.4...v0.30.5
[v0.30.4]: https://github.com/apache/incubator-opendal/compare/v0.30.3...v0.30.4
[v0.30.3]: https://github.com/apache/incubator-opendal/compare/v0.30.2...v0.30.3
[v0.30.2]: https://github.com/apache/incubator-opendal/compare/v0.30.1...v0.30.2
[v0.30.1]: https://github.com/apache/incubator-opendal/compare/v0.30.0...v0.30.1
[v0.30.0]: https://github.com/apache/incubator-opendal/compare/v0.29.1...v0.30.0
[v0.29.1]: https://github.com/apache/incubator-opendal/compare/v0.29.0...v0.29.1
[v0.29.0]: https://github.com/apache/incubator-opendal/compare/v0.28.0...v0.29.0
[v0.28.0]: https://github.com/apache/incubator-opendal/compare/v0.27.2...v0.28.0
[v0.27.2]: https://github.com/apache/incubator-opendal/compare/v0.27.1...v0.27.2
[v0.27.1]: https://github.com/apache/incubator-opendal/compare/v0.27.0...v0.27.1
[v0.27.0]: https://github.com/apache/incubator-opendal/compare/v0.26.2...v0.27.0
[v0.26.2]: https://github.com/apache/incubator-opendal/compare/v0.26.1...v0.26.2
[v0.26.1]: https://github.com/apache/incubator-opendal/compare/v0.26.0...v0.26.1
[v0.26.0]: https://github.com/apache/incubator-opendal/compare/v0.25.2...v0.26.0
[v0.25.2]: https://github.com/apache/incubator-opendal/compare/v0.25.1...v0.25.2
[v0.25.1]: https://github.com/apache/incubator-opendal/compare/v0.25.0...v0.25.1
[v0.25.0]: https://github.com/apache/incubator-opendal/compare/v0.24.6...v0.25.0
[v0.24.6]: https://github.com/apache/incubator-opendal/compare/v0.24.5...v0.24.6
[v0.24.5]: https://github.com/apache/incubator-opendal/compare/v0.24.4...v0.24.5
[v0.24.4]: https://github.com/apache/incubator-opendal/compare/v0.24.3...v0.24.4
[v0.24.3]: https://github.com/apache/incubator-opendal/compare/v0.24.2...v0.24.3
[v0.24.2]: https://github.com/apache/incubator-opendal/compare/v0.24.1...v0.24.2
[v0.24.1]: https://github.com/apache/incubator-opendal/compare/v0.24.0...v0.24.1
[v0.24.0]: https://github.com/apache/incubator-opendal/compare/v0.23.0...v0.24.0
[v0.23.0]: https://github.com/apache/incubator-opendal/compare/v0.22.6...v0.23.0
[v0.22.6]: https://github.com/apache/incubator-opendal/compare/v0.22.5...v0.22.6
[v0.22.5]: https://github.com/apache/incubator-opendal/compare/v0.22.4...v0.22.5
[v0.22.4]: https://github.com/apache/incubator-opendal/compare/v0.22.3...v0.22.4
[v0.22.3]: https://github.com/apache/incubator-opendal/compare/v0.22.2...v0.22.3
[v0.22.2]: https://github.com/apache/incubator-opendal/compare/v0.22.1...v0.22.2
[v0.22.1]: https://github.com/apache/incubator-opendal/compare/v0.22.0...v0.22.1
[v0.22.0]: https://github.com/apache/incubator-opendal/compare/v0.21.2...v0.22.0
[v0.21.2]: https://github.com/apache/incubator-opendal/compare/v0.21.1...v0.21.2
[v0.21.1]: https://github.com/apache/incubator-opendal/compare/v0.21.0...v0.21.1
[v0.21.0]: https://github.com/apache/incubator-opendal/compare/v0.20.1...v0.21.0
[v0.20.1]: https://github.com/apache/incubator-opendal/compare/v0.20.0...v0.20.1
[v0.20.0]: https://github.com/apache/incubator-opendal/compare/v0.19.8...v0.20.0
[v0.19.8]: https://github.com/apache/incubator-opendal/compare/v0.19.7...v0.19.8
[v0.19.7]: https://github.com/apache/incubator-opendal/compare/v0.19.6...v0.19.7
[v0.19.6]: https://github.com/apache/incubator-opendal/compare/v0.19.5...v0.19.6
[v0.19.5]: https://github.com/apache/incubator-opendal/compare/v0.19.4...v0.19.5
[v0.19.4]: https://github.com/apache/incubator-opendal/compare/v0.19.3...v0.19.4
[v0.19.3]: https://github.com/apache/incubator-opendal/compare/v0.19.2...v0.19.3
[v0.19.2]: https://github.com/apache/incubator-opendal/compare/v0.19.1...v0.19.2
[v0.19.1]: https://github.com/apache/incubator-opendal/compare/v0.19.0...v0.19.1
[v0.19.0]: https://github.com/apache/incubator-opendal/compare/v0.18.2...v0.19.0
[v0.18.2]: https://github.com/apache/incubator-opendal/compare/v0.18.1...v0.18.2
[v0.18.1]: https://github.com/apache/incubator-opendal/compare/v0.18.0...v0.18.1
[v0.18.0]: https://github.com/apache/incubator-opendal/compare/v0.17.4...v0.18.0
[v0.17.4]: https://github.com/apache/incubator-opendal/compare/v0.17.3...v0.17.4
[v0.17.3]: https://github.com/apache/incubator-opendal/compare/v0.17.2...v0.17.3
[v0.17.2]: https://github.com/apache/incubator-opendal/compare/v0.17.1...v0.17.2
[v0.17.1]: https://github.com/apache/incubator-opendal/compare/v0.17.0...v0.17.1
[v0.17.0]: https://github.com/apache/incubator-opendal/compare/v0.16.0...v0.17.0
[v0.16.0]: https://github.com/apache/incubator-opendal/compare/v0.15.0...v0.16.0
[v0.15.0]: https://github.com/apache/incubator-opendal/compare/v0.14.1...v0.15.0
[v0.14.1]: https://github.com/apache/incubator-opendal/compare/v0.14.0...v0.14.1
[v0.14.0]: https://github.com/apache/incubator-opendal/compare/v0.13.1...v0.14.0
[v0.13.1]: https://github.com/apache/incubator-opendal/compare/v0.13.0...v0.13.1
[v0.13.0]: https://github.com/apache/incubator-opendal/compare/v0.12.0...v0.13.0
[v0.12.0]: https://github.com/apache/incubator-opendal/compare/v0.11.4...v0.12.0
[v0.11.4]: https://github.com/apache/incubator-opendal/compare/v0.11.3...v0.11.4
[v0.11.3]: https://github.com/apache/incubator-opendal/compare/v0.11.2...v0.11.3
[v0.11.2]: https://github.com/apache/incubator-opendal/compare/v0.11.1...v0.11.2
[v0.11.1]: https://github.com/apache/incubator-opendal/compare/v0.11.0...v0.11.1
[v0.11.0]: https://github.com/apache/incubator-opendal/compare/v0.10.0...v0.11.0
[v0.10.0]: https://github.com/apache/incubator-opendal/compare/v0.9.1...v0.10.0
[v0.9.1]: https://github.com/apache/incubator-opendal/compare/v0.9.0...v0.9.1
[v0.9.0]: https://github.com/apache/incubator-opendal/compare/v0.8.0...v0.9.0
[v0.8.0]: https://github.com/apache/incubator-opendal/compare/v0.7.3...v0.8.0
[v0.7.3]: https://github.com/apache/incubator-opendal/compare/v0.7.2...v0.7.3
[v0.7.2]: https://github.com/apache/incubator-opendal/compare/v0.7.1...v0.7.2
[v0.7.1]: https://github.com/apache/incubator-opendal/compare/v0.7.0...v0.7.1
[v0.7.0]: https://github.com/apache/incubator-opendal/compare/v0.6.3...v0.7.0
[v0.6.3]: https://github.com/apache/incubator-opendal/compare/v0.6.2...v0.6.3
[v0.6.2]: https://github.com/apache/incubator-opendal/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com/apache/incubator-opendal/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com/apache/incubator-opendal/compare/v0.5.2...v0.6.0
[v0.5.2]: https://github.com/apache/incubator-opendal/compare/v0.5.1...v0.5.2
[v0.5.1]: https://github.com/apache/incubator-opendal/compare/v0.5.0...v0.5.1
[v0.5.0]: https://github.com/apache/incubator-opendal/compare/v0.4.2...v0.5.0
[v0.4.2]: https://github.com/apache/incubator-opendal/compare/v0.4.1...v0.4.2
[v0.4.1]: https://github.com/apache/incubator-opendal/compare/v0.4.0...v0.4.1
[v0.4.0]: https://github.com/apache/incubator-opendal/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/apache/incubator-opendal/compare/v0.2.5...v0.3.0
[v0.2.5]: https://github.com/apache/incubator-opendal/compare/v0.2.4...v0.2.5
[v0.2.4]: https://github.com/apache/incubator-opendal/compare/v0.2.3...v0.2.4
[v0.2.3]: https://github.com/apache/incubator-opendal/compare/v0.2.2...v0.2.3
[v0.2.2]: https://github.com/apache/incubator-opendal/compare/v0.2.1...v0.2.2
[v0.2.1]: https://github.com/apache/incubator-opendal/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/apache/incubator-opendal/compare/v0.1.4...v0.2.0
[v0.1.4]: https://github.com/apache/incubator-opendal/compare/v0.1.3...v0.1.4
[v0.1.3]: https://github.com/apache/incubator-opendal/compare/v0.1.2...v0.1.3
[v0.1.2]: https://github.com/apache/incubator-opendal/compare/v0.1.1...v0.1.2
[v0.1.1]: https://github.com/apache/incubator-opendal/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/apache/incubator-opendal/compare/v0.0.5...v0.1.0
[v0.0.5]: https://github.com/apache/incubator-opendal/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/apache/incubator-opendal/compare/v0.0.3...v0.0.4
[v0.0.3]: https://github.com/apache/incubator-opendal/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/apache/incubator-opendal/compare/v0.0.1...v0.0.2
