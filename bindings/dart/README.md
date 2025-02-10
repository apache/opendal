# Apache OpenDAL™ Dart Binding (WIP)

## Development

```
flutter pub get
flutter_rust_bridge_codegen generate
cd rust
cargo build -r
cd ..
dart run lib/opendal_test.dart
```

## Example

Dart stdlib

```dart
import 'dart:io';

void main() async {
  File myFile = File('myFile.txt');
  myFile.rename('yourFile.txt').then((_) => print('file renamed'));
}
```

Opendal

```dart
import 'src/rust/frb_generated.dart'; // change to import
import 'src/rust/api/opendal_api.dart';

Future<void> main() async {
  // Initialize the Rust bridge
  await RustLib.init();
  final op = new Operator(
    schemeStr: "fs",
    map: {"root": "/tmp"},
  );
  op.rename(from: "myFile.txt", to: "yourFile.txt").then((_) => print('file renamed'));
}

```

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
