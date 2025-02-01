import 'src/rust/frb_generated.dart';
import 'src/rust/api/opendal_api.dart';

Future<void> main() async {
  // Initialize the Rust bridge
  await RustLib.init();
}
