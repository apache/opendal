// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import 'dart:io';
import 'package:system_info2/system_info2.dart';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated_io.dart';
import 'src/rust/frb_generated.dart';
import 'src/rust/api/opendal_api.dart';
export 'src/rust/frb_generated.dart';
export 'src/rust/api/opendal_api.dart';

class Storage {
  final Operator _operator;

  Storage._(this._operator);

  static Future<Storage> init({
    required String schemeStr,
    required Map<String, String> map,
  }) async {
    if (!RustLib.instance.initialized) {
      var path = "rust/target/release/";  // default path
      final name = Platform.operatingSystem;
      final arch = SysInfo.kernelArchitecture;
      // if (name == "linux" && arch == "x86_64"){
      //   path = "rust/target/x86_64-unknown-linux-gnu/release/";
      // }
      // todo: more system and arch

      var config = ExternalLibraryLoaderConfig( // https://github.com/fzyzcjy/flutter_rust_bridge/issues/2460
        stem: 'opendal_dart',
        ioDirectory: path,
        webPrefix: 'pkg/',
      );
      await RustLib.init(externalLibrary: await loadExternalLibrary(config));
    }
    return Storage._(Operator(schemeStr: schemeStr, map: map));
  }

  /// Creates a factory function for creating File objects
  Function(String) initFile() {
    return (String path) => File._(path: path, operator: _operator);
  }

  /// Creates a factory function for creating Directory objects
  Function(String) initDir() {
    return (String path) => Directory._(path: path, operator: _operator);
  }
}

class File {
  final String path;
  final Operator _operator;

  File._({required this.path, required Operator operator})
      : _operator = operator;

  Future<bool> exists() {
    return _operator.isExist(path: path);
  }

  bool existsSync() {
    return _operator.isExistSync(path: path);
  }

  Future<Metadata> stat() {
    return _operator.stat(path: path);
  }

  Metadata statSync() {
    return _operator.statSync(path: path);
  }

  Future<void> delete() {
    return _operator.delete(path: path);
  }

  Future<void> rename(String newPath) {
    return _operator.rename(from: path, to: newPath);
  }

  void renameSync(String newPath) {
    _operator.renameSync(from: path, to: newPath);
  }

  void deleteSync() {
    _operator.deleteSync(path: path);
  }
}

class Directory {
  final String path;
  final Operator _operator;

  Directory._({required this.path, required Operator operator})
      : _operator = operator;

  Future<void> create() {
    return _operator.createDir(path: path);
  }

  void createSync() {
    _operator.createDirSync(path: path);
  }

  Future<bool> exists() {
    return _operator.isExist(path: path);
  }

  bool existsSync() {
    return _operator.isExistSync(path: path);
  }

  Future<void> rename(String newPath) {
    return _operator.rename(from: path, to: newPath);
  }

  void renameSync(String newPath) {
    _operator.renameSync(from: path, to: newPath);
  }

  Future<Metadata> stat() {
    return _operator.stat(path: path);
  }

  Metadata statSync() {
    return _operator.statSync(path: path);
  }

  Future<void> delete() {
    return _operator.delete(path: path);
  }

  void deleteSync() {
    _operator.deleteSync(path: path);
  }
}
