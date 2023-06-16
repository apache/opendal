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

public enum OperatorError: Error {
    case failedToBuild
    case operationError(UInt32)
}

/// A type used to access almost all OpenDAL APIs.
public class Operator<S: Service> {
    var nativeOp: opendal_operator_ptr
    
    deinit {
        withUnsafePointer(to: &nativeOp) { nativeOpPtr in
            opendal_operator_free(nativeOpPtr)
        }
    }
    
    /// Creates an operator with the given options.
    ///
    /// - Parameter options: The option map for creating the operator.
    /// - Throws: `OperatorError` value that indicates an error if failed.
    public init(options: [String : String]) throws {
        var nativeOptions = opendal_operator_options_new()
        nativeOp = withUnsafeMutablePointer(to: &nativeOptions) { nativeOptionsPtr in
            defer {
                opendal_operator_options_free(nativeOptionsPtr)
            }
            
            for option in options {
                opendal_operator_options_set(nativeOptionsPtr, option.key, option.value)
            }
            
            return opendal_operator_new(S.scheme, nativeOptionsPtr)
        }
        
        guard nativeOp.ptr != nil else {
            throw OperatorError.failedToBuild
        }
    }
    
    /// Blockingly write the data to a given path.
    ///
    /// - Parameter data: The content to be written.
    /// - Parameter path: The destination path for writing the data.
    /// - Throws: `OperatorError` value that indicates an error if failed to write.
    public func blockingWrite<D: COWData>(_ data: D, to path: String) throws {
        let code = data.withBorrowedPointer { dataPointer in
            // FIXME: The C binding has a bug that assumes any buffer are with
            // static lifetime, we must leak the buffer here as a workaround.
            let copied = UnsafeMutableBufferPointer<UInt8>.allocate(capacity: dataPointer.count)
            dataPointer.copyBytes(to: .init(start: copied.baseAddress, count: copied.count))
            
            let bytes = opendal_bytes(data: copied.baseAddress,
                                      len: UInt(copied.count))
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
    public func blockingRead(_ path: String) throws -> NativeData {
        let result = opendal_operator_blocking_read(nativeOp, path)
        guard result.code == OPENDAL_OK else {
            throw OperatorError.operationError(result.code.rawValue)
        }
        
        return .init(nativeBytesPtr: result.data)
    }
}
