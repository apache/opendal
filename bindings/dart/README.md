# Dart binding for opendal

## Development

```
flutter pub get
flutter_rust_bridge_codegen generate
cd rust
cargo build -r
cd ..
dart run lib/opendal_test.dart
```
