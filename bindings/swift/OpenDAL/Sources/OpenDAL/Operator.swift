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

        let ret = opendal_operator_new(scheme, nativeOptions)
        if let err = ret.error {
            defer {
                opendal_error_free(err)
            }
            let immutableErr = err.pointee
            let messagePointer = withUnsafePointer(to: immutableErr.message) { $0 }
            let messageLength = Int(immutableErr.message.len)
            throw OperatorError(
                code: immutableErr.code.rawValue,
                message: Data(bytes: messagePointer, count: messageLength)
            )
        }

        self.nativeOp = UnsafePointer(ret.op)!
    }

    public func blockingWrite(_ data: inout Data, to path: String) throws {
        let ret = data.withUnsafeMutableBytes { dataPointer in
            let address = dataPointer.baseAddress!.assumingMemoryBound(to: UInt8.self)
            let bytes = opendal_bytes(data: address, len: UInt(dataPointer.count), capacity: UInt(dataPointer.count))
            return withUnsafePointer(to: bytes) { bytesPointer in
                opendal_operator_write(nativeOp, path, bytesPointer)
            }
        }

        if let err = ret {
            defer {
                opendal_error_free(err)
            }
            let immutableErr = err.pointee
            let messagePointer = withUnsafePointer(to: immutableErr.message) { $0 }
            let messageLength = Int(immutableErr.message.len)
            throw OperatorError(
                code: immutableErr.code.rawValue,
                message: Data(bytes: messagePointer, count: messageLength)
            )
        }
    }

    public func blockingRead(_ path: String) throws -> Data {
        var ret = opendal_operator_read(nativeOp, path)
        if let err = ret.error {
            defer {
                opendal_error_free(err)
            }
            let immutableErr = err.pointee
            let messagePointer = withUnsafePointer(to: immutableErr.message) { $0 }
            let messageLength = Int(immutableErr.message.len)
            throw OperatorError(
                code: immutableErr.code.rawValue,
                message: Data(bytes: messagePointer, count: messageLength)
            )
        }

        return withUnsafeMutablePointer(to: &ret.data) { Data(openDALBytes: $0) }
    }
}
