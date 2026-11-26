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

/// A cursor over a nonrecursive listing. It can outlive the operator that created it.
/// Call `next()` serially on each cursor.
public final class BlockingLister {
    private var nativeLister: UnsafeMutablePointer<opendal_lister>?

    init(consuming lister: UnsafeMutablePointer<opendal_lister>) {
        self.nativeLister = lister
    }

    deinit {
        close()
    }

    /// Returns the next entry, or `nil` when the listing is exhausted.
    /// Throws `OperatorError` if advancing the listing fails.
    /// After returning `nil` or throwing, all later calls return `nil`.
    public func next() throws -> Entry? {
        guard let lister = nativeLister else { return nil }

        var result: UnsafeMutablePointer<opendal_entry>? = nil
        if let err = opendal_lister_next(lister, &result) {
            close()
            throw OperatorError(consuming: err)
        }
        guard let entry = result else {
            close()
            return nil
        }
        return Entry(consuming: entry)
    }

    private func close() {
        guard let lister = nativeLister else { return }
        nativeLister = nil
        opendal_lister_free(lister)
    }
}
