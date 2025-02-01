import 'package:test/test.dart';
import 'src/rust/frb_generated.dart';
import 'src/rust/api/opendal_api.dart';

void main() {
  group('opendal unit test', () {
    group('opendal fs schema', () {
      test('operator function in fs schema', () async {
        await RustLib.init();
        final op = new Operator(
          schemeStr: "fs",
          map: {"root": "/tmp"},
        );
        expect(op, isNotNull);

        await op.createDir(path: "test_dir/");
        expect(await op.isExist(path: "test_dir/"), true);

        expect(await op.isExist(path: "test_1.txt"), false);
        expect(await op.isExist(path: "test.txt"), false);
      });
    });

    group('opendal memory schema', () {
      test('operator function in memory schema', () async {
        final op = new Operator(
          schemeStr: "memory",
          map: {"root": "/tmp"},
        );
        expect(op, isNotNull);
      });

      test('meta function in memory schema', () async {
        final op = new Operator(
          schemeStr: "memory",
          map: {"root": "/tmp"},
        );
        expect(op, isNotNull);

        await op.createDir(path: "test/");

        final meta = await op.stat(path: "test/");
        expect(meta, isNotNull);

        final isFile = meta.isFile;
        expect(isFile, false);

        final isDir = meta.isDirectory;
        expect(isDir, true);
      });
    });
  });
}
