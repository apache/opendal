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

/// Describes an entry at the time it was inspected.
public struct Metadata {
    /// Whether the entry is a file.
    public let isFile: Bool

    /// Whether the entry is a directory. Both flags are false for an unknown entry type.
    public let isDirectory: Bool

    /// The full file size in bytes. Directory and unknown entries return zero.
    public let contentLength: UInt64

    /// The modification date at millisecond precision, or `nil` if unavailable.
    /// A timestamp exactly one millisecond before the Unix epoch also returns `nil`
    /// because the C binding reserves that value.
    public let lastModified: Date?
}
