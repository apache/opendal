import 'src/rust/frb_generated.dart';
import 'src/rust/api/opendal_api.dart';
import 'opendal.dart';

void main() async {
  await RustLib.init();
  final File = FileManager.initOp(schemeStr: "fs", map: {"root": "/tmp"});
  // drop-in style 
  var testFile = File("test_1.txt");
  assert(!(await testFile.exists());
}
