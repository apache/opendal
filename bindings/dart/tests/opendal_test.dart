import 'package:test/test.dart';
import '../lib/opendal.dart';

void main() {
  group('opendal unit test', () {
    group('opendal fs schema', () {
      test('File and Directory functions in fs schema', () async {
        final storage = await Storage.init(schemeStr: "fs", map: {"root": "/tmp"});
        final File = storage.initFile();
        var testFile = File("test_1.txt");
        expect(await testFile.exists(), false);

        var anotherFile = File("test.txt");
        expect(await anotherFile.exists(), false);

        final Directory = storage.initDir();

        var testDir = Directory("test_dir/");
        await testDir.create();
        expect(await testDir.exists(), true);
      });
    });

    group('opendal memory schema', () {
      test('File and Directory functions in memory schema', () async {
        final storage = await Storage.init(schemeStr: "memory", map: {"root": "/tmp"});
        final Directory = storage.initDir();

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
