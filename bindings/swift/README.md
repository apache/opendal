# Apache OpenDAL™ Swift Binding (WIP)

[![status: unreleased](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/docs/bindings/swift)

A Swift binding for OpenDAL. Access storage services through one consistent
API backed by the Rust core. **Experimental — not suitable for production use.**

We release the OpenDAL Swift binding independently of the
[`opendal` crate](https://crates.io/crates/opendal) (Rust core). For updates
and compatibility, use the Swift binding version instead of the `opendal` crate
version.

## Useful Links

- **User guide**: https://opendal.apache.org/docs/bindings/swift
- **Services & configuration**: https://opendal.apache.org/services

## Build

Build and stage the C header and static library from the same checkout as the
Swift package. Run these commands from the repository root.

```shell
make -C bindings/swift build-c
```

The C library builds in debug mode by default. To build an optimized C library,
set `OPENDAL_C_PROFILE=release`.

```shell
make -C bindings/swift build-c OPENDAL_C_PROFILE=release
```

You can combine this option with `OPENDAL_FEATURES` to enable additional
services. SwiftPM's `-c release` selects the Swift build configuration
separately. If you already built your app, run `swift package clean` in the app
directory before rebuilding to link against the newly staged library.

The `test` target stages the C library before running the Swift tests.
Replace `build-c` with `test` in your build command and keep the same
`OPENDAL_C_PROFILE` and `OPENDAL_FEATURES` options. If switching builds,
first run `swift package clean` in `bindings/swift/OpenDAL`.

## Installation

The package is not published to any registry. In your project's `Package.swift`,
use the local path to `bindings/swift/OpenDAL` in the checkout where you ran
`make build-c`.

```swift
// swift-tools-version:5.7
import PackageDescription

let package = Package(
    name: "MyTool",
    dependencies: [
        .package(path: "/path/to/opendal/bindings/swift/OpenDAL"),
    ],
    targets: [
        .target(name: "MyTool", dependencies: [
            .product(name: "OpenDAL", package: "OpenDAL"),
        ]),
    ]
)
```

## Quickstart

```swift
import OpenDAL

// Create an operator backed by the in-memory service.
let op = try Operator(scheme: "memory")

// Write bytes to a path.
var data = Data([1, 2, 3, 4])
try op.blockingWrite(&data, to: "/demo")

// Read them back.
let result = try op.blockingRead("/demo")
print(result)
```

Both `blockingWrite` and `blockingRead` throw `OperatorError` on failure.

The default build includes only the Memory service. To enable S3, run the
following command from the repository root.

```shell
make -C bindings/swift build-c OPENDAL_FEATURES=opendal/services-s3
```

Then create an operator with the S3 scheme and options.

```swift
let op = try Operator(
    scheme: "s3",
    options: [
        "bucket": "my-bucket",
        "region": "us-east-1",
    ]
)
```

See the [user guide](https://opendal.apache.org/docs/bindings/swift) for more
examples, and [Services](https://opendal.apache.org/services) for the full list
of backends and their configuration keys.

## List entries

Use `blockingLister` to open a nonrecursive listing. Use a trailing slash
for a named directory.

```swift
let lister = try op.blockingLister("logs/")
while let entry = try lister.next() {
    print(entry.path)
}
```

Opening the cursor and reading its next entry can both throw `OperatorError`.
After `next()` returns `nil` or throws, later calls return `nil`. You can break
out of the loop early. Releasing the cursor closes the listing, and entries
remain usable after releasing the cursor and operator.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to set up the development
environment and submit patches.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or
trademarks of the Apache Software Foundation.
