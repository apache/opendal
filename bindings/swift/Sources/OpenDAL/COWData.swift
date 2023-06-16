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

/// A copy-on-write data type, which can eliminate copies when
/// being borrowed.
public protocol COWData {
    /// Calls the given closure with a pointer to the underlying data.
    func withBorrowedPointer<R>(_ body: (UnsafeBufferPointer<UInt8>) -> R) -> R
    
    /// Returns an owned `Data` by copying the content bytes of
    /// this instance.
    func copy() -> Data
}

public extension COWData {
    func copy() -> Data {
        return withBorrowedPointer { borrowedPointer in
            return Data(bytes: borrowedPointer.baseAddress!, count: borrowedPointer.count)
        }
    }
}

public extension COWData {
    /// Calls the given closure with a `Data` view to the underlying data.
    ///
    /// **Safety Note:**
    /// The given `Data` must not outlive this instance and should only
    /// be used inside the closure.
    func withBorrowedData<R>(_ body: (Data) -> R) -> R {
        return withBorrowedPointer { borrowedPointer in
            let borrowedData = Data(bytesNoCopy: .init(mutating: borrowedPointer.baseAddress!),
                                    count: borrowedPointer.count,
                                    deallocator: .none)
            return body(borrowedData)
        }
    }
}

/// A managed reference to the data object allocated in Rust.
///
/// This object can be used to read data from Rust with zero-copying.
/// The underlying buffer will be freed when this object deallocates.
public class NativeData: COWData {
    var nativeBytesPtr: UnsafePointer<opendal_bytes>
    
    deinit {
        opendal_bytes_free(nativeBytesPtr)
    }
    
    init(nativeBytesPtr: UnsafePointer<opendal_bytes>) {
        self.nativeBytesPtr = nativeBytesPtr
    }
    
    public func withBorrowedPointer<R>(_ body: (UnsafeBufferPointer<UInt8>) -> R) -> R {
        let nativeBytes = nativeBytesPtr.pointee
        return body(.init(start: nativeBytes.data, count: Int(nativeBytes.len)))
    }
}

extension Data: COWData {
    public func withBorrowedPointer<R>(_ body: (UnsafeBufferPointer<UInt8>) -> R) -> R {
        return self.withUnsafeBytes { pointer in
            return body(pointer.assumingMemoryBound(to: UInt8.self))
        }
    }
}

extension UnsafeBufferPointer<UInt8>: COWData {
    public func withBorrowedPointer<R>(_ body: (UnsafeBufferPointer<UInt8>) -> R) -> R {
        return body(self)
    }
}
