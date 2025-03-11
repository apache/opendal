import 'package:test/test.dart';
import '../lib/opendal.dart';

void main() {
  group('opendal unit test', () {
    group('opendal fs schema', () {
      test('File and Directory functions in fs schema', () async {
        await RustLib.init();
        final File = FileManager.initOp(schemeStr: "fs", map: {"root": "/tmp"});
        var testFile = File("test_1.txt");
        expect(await testFile.exists(), false);

        var anotherFile = File("test.txt");
        expect(await anotherFile.exists(), false);

        final Directory =
            DirectoryManager.initOp(schemeStr: "fs", map: {"root": "/tmp"});
        var testDir = Directory("test_dir/");
        await testDir.create();
        expect(await testDir.exists(), true);
      });
    });

    group('opendal memory schema', () {
      test('File and Directory functions in memory schema', () async {
        final File =
            FileManager.initOp(schemeStr: "memory", map: {"root": "/tmp"});
        final Directory =
            DirectoryManager.initOp(schemeStr: "memory", map: {"root": "/tmp"});
        var testDir = Directory("test/");
        await testDir.create();
        expect(
            await testDir.exists(), isTrue); // Directory exists after creation

        final meta = await testDir.stat();
        expect(meta, isNotNull);
        expect(meta.isFile, false);
        expect(meta.isDirectory, true);
      });
    });
  });
}
