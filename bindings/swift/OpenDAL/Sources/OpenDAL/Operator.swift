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

import COpenDAL
import Foundation

public struct OperatorError: Error {
    let code: UInt32
    let message: Data

    /// Creates an `OperatorError` by copying the code and message out
    /// of a `opendal_error` and freeing it afterwards.
    ///
    /// The caller transfers ownership of `error` to this initializer;
    /// it must not be used or freed again afterwards.
    init(consuming error: UnsafeMutablePointer<opendal_error>) {
        self.code = error.pointee.code.rawValue
        if let data = error.pointee.message.data, error.pointee.message.len > 0 {
            self.message = Data(bytes: data, count: Int(error.pointee.message.len))
        }
        else {
            self.message = Data()
        }
        opendal_error_free(error)
    }

    fileprivate init(code: opendal_code, message: String) {
        self.code = code.rawValue
        self.message = Data(message.utf8)
    }
}

public class Operator {
    var nativeOp: UnsafePointer<opendal_operator>

    deinit {
        opendal_operator_free(nativeOp)
    }

    public init(scheme: String, options: [String: String] = [:]) throws {
        let nativeOptions = opendal_operator_options_new()
        defer {
            opendal_operator_options_free(nativeOptions)
        }

        for option in options {
            opendal_operator_options_set(nativeOptions, option.key, option.value)
        }

        var op: UnsafeMutablePointer<opendal_operator>? = nil
        if let err = opendal_operator_new(scheme, nativeOptions, &op) {
            throw OperatorError(consuming: err)
        }

        self.nativeOp = UnsafePointer(op!)
    }

    public func blockingWrite(_ data: inout Data, to path: String) throws {
        let ret = data.withUnsafeMutableBytes { dataPointer in
            // The C binding rejects an empty buffer with a non-null data
            // pointer, so map empty Data to a fully empty opendal_bytes.
            let bytes =
                dataPointer.isEmpty
                ? opendal_bytes()
                : opendal_bytes(
                    data: dataPointer.baseAddress?.assumingMemoryBound(to: UInt8.self),
                    len: UInt(dataPointer.count),
                    capacity: UInt(dataPointer.count)
                )
            return withUnsafePointer(to: bytes) { bytesPointer in
                opendal_operator_write(nativeOp, path, bytesPointer)
            }
        }

        if let err = ret {
            throw OperatorError(consuming: err)
        }
    }

    public func blockingRead(_ path: String) throws -> Data {
        var data = opendal_bytes()
        if let err = opendal_operator_read(nativeOp, path, &data) {
            throw OperatorError(consuming: err)
        }

        return Data(openDALBytes: data)
    }

    /// Returns metadata for an existing path.
    /// Use a trailing slash for a named directory. The service must support stat.
    /// Throws `OperatorError` on failure, including a missing path or a path
    /// containing a NUL character (U+0000).
    public func blockingStat(_ path: String) throws -> Metadata {
        try Self.validatePath(path)

        var result: UnsafeMutablePointer<opendal_metadata>? = nil
        if let err = opendal_operator_stat(nativeOp, path, &result) {
            throw OperatorError(consuming: err)
        }
        guard let metadata = result else {
            throw OperatorError(code: OPENDAL_UNEXPECTED, message: "Stat returned no metadata.")
        }
        return Metadata(consuming: metadata)
    }

    /// Opens a cursor for a nonrecursive listing at the given path.
    /// Use a trailing slash to list a named directory. Some services also
    /// return an entry for the directory itself. The service must support list.
    /// Throws `OperatorError` if opening the cursor fails or the path contains
    /// a NUL character (U+0000). Advancing the cursor can also throw.
    ///
    /// ```swift
    /// let lister = try op.blockingLister("logs/")
    /// while let entry = try lister.next() {
    ///     print(entry.path)
    /// }
    /// ```
    public func blockingLister(_ path: String) throws -> BlockingLister {
        try Self.validatePath(path)

        var result: UnsafeMutablePointer<opendal_lister>? = nil
        if let err = opendal_operator_list(nativeOp, path, &result) {
            throw OperatorError(consuming: err)
        }
        guard let lister = result else {
            throw OperatorError(code: OPENDAL_UNEXPECTED, message: "List returned no lister.")
        }
        return BlockingLister(consuming: lister)
    }

    /// Returns `true` if the path exists or `false` if it is missing.
    /// The service must support stat. Throws `OperatorError` on failure or
    /// if the path contains a NUL character (U+0000).
    public func blockingExists(_ path: String) throws -> Bool {
        try Self.validatePath(path)

        var exists = false
        if let err = opendal_operator_exists(nativeOp, path, &exists) {
            throw OperatorError(consuming: err)
        }

        return exists
    }

    /// Deletes the path without recursion. Deleting a missing file succeeds.
    /// The service must support delete. Throws `OperatorError` on failure or
    /// if the path contains a NUL character (U+0000).
    public func blockingDelete(_ path: String) throws {
        try Self.validatePath(path)

        if let err = opendal_operator_delete(nativeOp, path) {
            throw OperatorError(consuming: err)
        }
    }

    /// Creates the directory and any missing parents. Succeeds if the directory already exists.
    /// Use a trailing slash for a named directory (for example, `logs/archive/`).
    /// The service must support directory creation. Throws `OperatorError` on failure or
    /// if the path contains a NUL character (U+0000).
    public func blockingCreateDirectory(_ path: String) throws {
        try Self.validatePath(path)

        if let err = opendal_operator_create_dir(nativeOp, path) {
            throw OperatorError(consuming: err)
        }
    }

    /// Copies a file within this operator, overwriting an existing destination file.
    /// The service must support copy. Throws `OperatorError` on failure, including
    /// directory paths, identical normalized paths, or embedded NUL characters
    /// (U+0000) in either path.
    public func blockingCopy(_ source: String, to destination: String) throws {
        try Self.validatePath(source)
        try Self.validatePath(destination)

        if let err = opendal_operator_copy(nativeOp, source, destination) {
            throw OperatorError(consuming: err)
        }
    }

    /// Moves a file within this operator, overwriting an existing destination file.
    /// The service must support rename. Throws `OperatorError` on failure, including
    /// directory paths, identical normalized paths, or embedded NUL characters
    /// (U+0000) in either path.
    public func blockingRename(_ source: String, to destination: String) throws {
        try Self.validatePath(source)
        try Self.validatePath(destination)

        if let err = opendal_operator_rename(nativeOp, source, destination) {
            throw OperatorError(consuming: err)
        }
    }

    private static func validatePath(_ path: String) throws {
        // The C API treats the first NUL byte as the end of the path.
        if path.utf8.contains(0) {
            throw OperatorError(
                code: OPENDAL_UNEXPECTED,
                message: "Path contains a NUL character (U+0000)."
            )
        }
    }
}
