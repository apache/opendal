# Apache OpenDAL™ Dart Binding (WIP)

## Useful Links

- [Examples](./examples)

## Usage

Api is designed to be like stdlib style.

This is stdlib

```dart
import 'dart:io';

void main() async {
  final file = File('file.txt');
  var is_exists = await file.exists();
  print(is_exists);
}
```

This is opendal

```dart
import 'package:opendal/opendal.dart';

void main() async {
  final storage = await Storage.init(schemeStr: "fs", map: {"root": "/tmp"});
  final File = storage.initFile();
  // drop-in
  final file = File('file.txt');
  var is_exists = await file.exists();
  print(is_exists);
}

```

## Test

```
dart run tests/opendal_test.dart
```

## Development

```
flutter pub get
flutter_rust_bridge_codegen generate
cd rust
cargo build -r
```

## Update generated code

This binding uses <https://github.com/fzyzcjy/flutter_rust_bridge>, when updating the codegen. First check `FLUTTER_RUST_BRIDGE_CODEGEN_VERSION`, then pin the version of `flutter_rust_bridge` in `pubspec.yaml` and `rust/Cargo.toml`. Make sure the runtime versions are matched.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
