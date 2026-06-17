---
title: Getting started
sidebar_label: Getting started
description: Build and run your first OpenDAL program in Dart — write a file and read it back.
---

# Getting started

## Prerequisites

The Dart binding wraps the Rust core via `flutter_rust_bridge`. You need to
build the native library before running Dart code.

```shell
# From the repository root
cd bindings/dart/rust
cargo build -r
cd ..
dart pub get
```

The loader picks up the native library from `rust/target/release/` automatically
for native (non-cross-compiled) builds.

## Your first program

This program creates an in-memory storage, writes a file, reads it back, and
checks metadata. It uses the `memory` service so there are no credentials or
paths to configure.

```dart
import 'dart:typed_data';
import 'package:opendal/opendal.dart';

void main() async {
  // Initialize storage with a service scheme and configuration map.
  final storage = await Storage.init(schemeStr: "memory", map: {"root": "/"});

  // initFile() returns a factory function that mirrors dart:io File.
  final File = storage.initFile();

  final file = File("hello.txt");

  // Write bytes to the file.
  await file.write(Uint8List.fromList("Hello, OpenDAL!".codeUnits));

  // Read bytes back.
  final data = await file.read();
  print(String.fromCharCodes(data)); // Hello, OpenDAL!

  // Inspect metadata.
  final meta = await file.stat();
  print("isFile: ${meta.isFile}");
  print("contentLength: ${meta.contentLength}");

  // Delete the file.
  await file.delete();
}
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

`storage.initDir()` returns a factory for `Directory` objects:

```dart
final storage = await Storage.init(schemeStr: "memory", map: {"root": "/"});
final Directory = storage.initDir();

final dir = Directory("my-dir/");
await dir.create();
print(await dir.exists()); // true

final meta = await dir.stat();
print("isDirectory: ${meta.isDirectory}"); // true
```
