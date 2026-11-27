---
title: Getting started
sidebar_label: Getting started
description: Build and run your first OpenDAL program in Dart — write a file and read it back.
---

# Getting started

## Prerequisites

Install Git, Rust, and Dart (3.9.2 or later in the 3.x series). Clone OpenDAL
and build the native library before running Dart code.

```shell
git clone https://github.com/apache/opendal.git
cd opendal/bindings/dart/rust
cargo build -r
cd ..
dart pub get
```

## Your first program

Run the checked-in example from `bindings/dart/`. For a default Cargo build,
`Storage.init()` loads the native library from `rust/target/release/` relative
to the working directory.

```shell
dart run examples/getting_started.dart
```

This program creates an in-memory storage, writes a file, reads it back, and
checks metadata. It uses the `memory` service so there are no credentials or
paths to configure.

```dart file=bindings/dart/examples/getting_started.dart region=quickstart
```

`read()` returns `Uint8List`. `write()` takes `Uint8List`; a plain `List<int>` must be converted first with `Uint8List.fromList(list)`.

`meta.contentLength` is typed `BigInt?`, not `int`. Use `.toInt()` or `.toString()` if you need a plain integer or a string.

## Point it at a real backend

Only `Storage.init` changes — the `File` and `Directory` API stays the same:

```dart
// Local filesystem rooted at /tmp
final storage = await Storage.init(schemeStr: "fs", map: {"root": "/tmp"});
final File = storage.initFile();

final file = File("test.txt");
await file.write(Uint8List.fromList("Hello from fs!".codeUnits));
print(await file.exists()); // true
```

For S3 or other services, pass the scheme and any configuration keys that
service requires — see [Services](/services) for the full list.

## Sync variants

Every async method has a blocking `*Sync` counterpart. Use them when you cannot
`await`:

```dart
final storage = await Storage.init(schemeStr: "memory", map: {"root": "/"});
final File = storage.initFile();
final file = File("sync.txt");

file.writeSync(Uint8List.fromList([1, 2, 3]));
final data = file.readSync();
print(data); // [1, 2, 3]
```

## Working with directories

Reuse the same `Storage` for files and directories that use the same service
and configuration. `storage.initDir()` returns a factory for `Directory` objects.

```dart
final Directory = storage.initDir();

final dir = Directory("my-dir/");
await dir.create();
print(await dir.exists()); // true

final dirMeta = await dir.stat();
print("isDirectory: ${dirMeta.isDirectory}"); // true
```
