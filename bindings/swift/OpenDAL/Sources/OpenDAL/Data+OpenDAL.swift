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

extension Data {
    /// Creates a new data by managing `opendal_bytes` as its
    /// underlying buffer.
    ///
    /// This can be used to read data from Rust with zero-copying.
    /// The underlying buffer will be freed when the data gets
    /// deallocated.
    init(openDALBytes: UnsafeMutablePointer<opendal_bytes>) {
        let address = UnsafeRawPointer(openDALBytes.pointee.data)!
        let length = Int(openDALBytes.pointee.len)
        self.init(
            bytesNoCopy: .init(mutating: address),
            count: length,
            deallocator: .custom({ _, _ in
                opendal_bytes_free(openDALBytes)
            })
        )
    }
}
