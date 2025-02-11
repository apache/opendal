import 'src/rust/frb_generated.dart';
import 'src/rust/api/opendal_api.dart';

class OperatorManager {
  static Operator? _operator;
  
  static void initOperator({required String schemeStr, required Map<String, String> map}) {
    if (_operator == null) {
      _operator = Operator(schemeStr: schemeStr, map: map);
    } else {
      print("Warning: Operator is already initialized.");
    }
  }
  
  static Operator get operator {
    if (_operator == null) {
      throw StateError('Operator has not been initialized. Call initOperator first.');
    }
    return _operator!;
  }
}

class File {
  final String path;

  File(this.path);

  static void initOperator({required String schemeStr, required Map<String, String> map}) {
    OperatorManager.initOperator(schemeStr: schemeStr, map: map);
  }

  Future<bool> exists() {
    return OperatorManager.operator.isExist(path: path);
  }

  bool existsSync() {
    return OperatorManager.operator.isExistSync(path: path);
  }

  Future<Metadata> stat() {
    return OperatorManager.operator.stat(path: path);
  }

  Metadata statSync() {
    return OperatorManager.operator.statSync(path: path);
  }

  Future<void> delete() {
    return OperatorManager.operator.delete(path: path);
  }

  Future<void> rename(String newPath) {
    return OperatorManager.operator.rename(from: path, to: newPath);
  }

  void renameSync(String newPath) {
    OperatorManager.operator.renameSync(from: path, to: newPath);
  }

  void deleteSync() {
    OperatorManager.operator.deleteSync(path: path);
  }
}

class Directory {
  final String path;

  Directory(this.path);

  static void initOperator({required String schemeStr, required Map<String, String> map}) {
    OperatorManager.initOperator(schemeStr: schemeStr, map: map);
  }

  Future<void> create() {
    return OperatorManager.operator.createDir(path: path);
  }

  void createSync() {
    OperatorManager.operator.createDirSync(path: path);
  }

  Future<bool> exists() {
    return OperatorManager.operator.isExist(path: path);
  }

  bool existsSync() {
    return OperatorManager.operator.isExistSync(path: path);
  }

  Future<void> rename(String newPath) {
    return OperatorManager.operator.rename(from: path, to: newPath);
  }

  void renameSync(String newPath) {
    OperatorManager.operator.renameSync(from: path, to: newPath);
  }

  Future<Metadata> stat() {
    return OperatorManager.operator.stat(path: path);
  }

  Metadata statSync() {
    return OperatorManager.operator.statSync(path: path);
  }

  Future<void> delete() {
    return OperatorManager.operator.delete(path: path);
  }

  void deleteSync() {
    OperatorManager.operator.deleteSync(path: path);
  }
}