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
}
