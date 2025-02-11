import 'package:test/test.dart';
import 'src/rust/frb_generated.dart';
import 'src/rust/api/opendal_api.dart';
import 'opendal.dart';

void main() {
  group('opendal unit test', () {
    group('opendal fs schema', () {
      setUpAll(() async {
        await RustLib.init();
        File.initOperator(schemeStr: "fs", map: {"root": "/tmp"});
      });
      test('File and Directory functions in fs schema', () async {
        var testFile = File("test_1.txt");
        expect(await testFile.exists(), false);

        var anotherFile = File("test.txt");
        expect(await anotherFile.exists(), false);


        var testDir = Directory("test_dir/");
        await testDir.create();
        expect(await testDir.exists(), true);
      });
    });

    group('opendal memory schema', () {
      setUpAll(() async {
        Directory.initOperator(schemeStr: "memory", map: {"root": "/tmp"});
      });

      test('File and Directory functions in memory schema', () async {
        var testDir = Directory("test/");
        await testDir.create();
        expect(await testDir.exists(), isTrue); // Directory exists after creation


        final meta = await testDir.stat();
        expect(meta, isNotNull);
        expect(meta.isFile, false);
        expect(meta.isDirectory, true);
      });
    });
  });
}