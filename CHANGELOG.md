# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v0.2.1] - 2022-03-10

### Added

- *: Implement logging support (#122)
- feat(service): Add service memory read support (#121)
- services: Add basic metrics (#127)
- services: Add full memory support (#134)

### Changed

- benches: Refactor to support more read pattern (#126)
- services: Refactor into directories (#131)

### Docs

- docs: Cover all public types and functions (#128)
- docs: Update README (#129)
- ci: Generate main docs to <opendal.databend.rs> (#132)
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

- [limited_reader](https://github.com/datafuselabs/opendal/blob/main/docs/rfcs/0090-limited-reader.md)

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
- *: Refactor tests (#60)

## [v0.0.5] - 2022-02-23

### Fixed

- io: Remove not used debug print (#48)

## [v0.0.4] - 2022-02-23

### Added

- readers: Allow config prefetch size (#31)
- RFC-0041: Object Native API (#41)
- *: Implement RFC-0041 Object Native API (#35)
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
- 
### Fixed

- services/s3: Allow set endpoint and region while input value is valid (#17)

## v0.0.1 - 2022-02-14

### Added

Hello, OpenDAL!

[v0.2.1]: https://github.com/datafuselabs/opendal/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/datafuselabs/opendal/compare/v0.1.4...v0.2.0
[v0.1.4]: https://github.com/datafuselabs/opendal/compare/v0.1.3...v0.1.4
[v0.1.3]: https://github.com/datafuselabs/opendal/compare/v0.1.2...v0.1.3
[v0.1.2]: https://github.com/datafuselabs/opendal/compare/v0.1.1...v0.1.2
[v0.1.1]: https://github.com/datafuselabs/opendal/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/datafuselabs/opendal/compare/v0.0.5...v0.1.0
[v0.0.5]: https://github.com/datafuselabs/opendal/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/datafuselabs/opendal/compare/v0.0.3...v0.0.4
[v0.0.3]: https://github.com/datafuselabs/opendal/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/datafuselabs/opendal/compare/v0.0.1...v0.0.2