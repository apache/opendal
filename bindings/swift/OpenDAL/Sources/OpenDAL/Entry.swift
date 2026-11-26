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

/// A listed entry. Its values remain available after the cursor and operator are released.
public struct Entry {
    /// The path relative to the operator's root.
    public let path: String

    /// The last segment of the path, including the trailing slash for a directory.
    public let name: String

    /// The metadata returned by the listing for this entry.
    public let metadata: Metadata

    init(consuming entry: UnsafeMutablePointer<opendal_entry>) {
        defer { opendal_entry_free(entry) }
        let path = opendal_entry_path(entry)!
        defer { opendal_string_free(path) }
        let name = opendal_entry_name(entry)!
        defer { opendal_string_free(name) }

        self.path = String(cString: path)
        self.name = String(cString: name)
        self.metadata = Metadata(consuming: opendal_entry_metadata(entry))
    }
}
