# Apache OpenDAL™ Swift Binding (WIP)

![](https://img.shields.io/badge/status-unreleased-red)

![](https://github.com/apache/incubator-opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Using the Package

### Build C Dependencies

The Swift binding depends on the C binding to OpenDAL. Before using this package, you need to build the C library first:

```
cd bindings/swift
make build-c
```

To check whether the package is ready to use, simply run the test:

```
make test
```

### Add Dependency to Your Project

The package manifest is not located at the root directory of its repository. To use it, add the path of this package to the `Package.swift` manifest of your project:

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

## Example

The demo below shows how to write a key to the memory storage, and read it back:

```swift
import OpenDAL

// Create an operator with `memory` backend.
let op = try Operator(scheme: "memory")

// Write some data into path `/demo`.
let someData = Data([1, 2, 3, 4])
try op.blockingWrite(someData, to: "/demo")

// Read the data back.
let readData = try op.blockingRead("/demo")

// You can use the read data here.
print(readData!)
```

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.

