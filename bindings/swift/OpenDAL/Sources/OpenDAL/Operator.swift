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

import Foundation
import COpenDAL

public enum OperatorError: Error {
    case failedToBuild
    case operationError(UInt32)
}

/// A type used to access almost all OpenDAL APIs.
public class Operator {
    var nativeOp: UnsafePointer<opendal_operator_ptr>
    
    deinit {
        opendal_operator_free(nativeOp)
    }
    
    /// Creates an operator with the given options.
    ///
    /// - Parameter options: The option map for creating the operator.
    /// - Throws: `OperatorError` value that indicates an error if failed.
    public init(scheme: String, options: [String : String] = [:]) throws {
        let nativeOptions = opendal_operator_options_new()
        defer {
            opendal_operator_options_free(nativeOptions)
        }
            
        for option in options {
            opendal_operator_options_set(nativeOptions, option.key, option.value)
        }
        
        guard let nativeOp = opendal_operator_new(scheme, nativeOptions) else {
            throw OperatorError.failedToBuild
        }
        
        guard nativeOp.pointee.ptr != nil else {
            throw OperatorError.failedToBuild
        }

        self.nativeOp = nativeOp
    }
    
    /// Blockingly write the data to a given path.
    ///
    /// - Parameter data: The content to be written.
    /// - Parameter path: The destination path for writing the data.
    /// - Throws: `OperatorError` value that indicates an error if failed to write.
    public func blockingWrite(_ data: Data, to path: String) throws {
        let code = data.withUnsafeBytes { dataPointer in
            let address = dataPointer.baseAddress!.assumingMemoryBound(to: UInt8.self)
            let bytes = opendal_bytes(data: address, len: UInt(dataPointer.count))
            return opendal_operator_blocking_write(nativeOp, path, bytes)
        }
        
        guard code == OPENDAL_OK else {
            throw OperatorError.operationError(code.rawValue)
        }
    }
    
    /// Blockingly read the data from a given path.
    ///
    /// - Parameter path: Path of the data to read.
    /// - Returns: `NativeData` object if the data exists.
    /// - Throws: `OperatorError` value that indicates an error if failed to read.
    public func blockingRead(_ path: String) throws -> Data? {
        let result = opendal_operator_blocking_read(nativeOp, path)
        guard result.code == OPENDAL_OK else {
            throw OperatorError.operationError(result.code.rawValue)
        }
        
        return .init(openDALBytes: result.data)
    }
}
