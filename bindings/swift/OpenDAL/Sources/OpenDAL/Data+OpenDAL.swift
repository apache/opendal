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
    /// Creates a new data by taking over `bytes` as its
    /// underlying buffer.
    ///
    /// This can be used to read data from Rust with zero-copying.
    /// `bytes` is moved into a heap-allocated box whose lifetime is
    /// managed by the data's deallocator, so both the box and the
    /// Rust buffer it owns are freed when the data gets deallocated.
    init(openDALBytes bytes: opendal_bytes) {
        // Empty results carry no buffer, free `bytes` eagerly and
        // produce an empty Data.
        guard let data = bytes.data, bytes.len > 0 else {
            var bytes = bytes
            opendal_bytes_free(&bytes)
            self.init()
            return
        }

        let length = Int(bytes.len)
        let box = UnsafeMutablePointer<opendal_bytes>.allocate(capacity: 1)
        box.initialize(to: bytes)
        self.init(
            bytesNoCopy: .init(mutating: data),
            count: length,
            deallocator: .custom({ _, _ in
                opendal_bytes_free(box)
                box.deinitialize(count: 1)
                box.deallocate()
            })
        )
    }
}
