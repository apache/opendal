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

const opendal = @import("opendal");
const std = @import("std");
const testing = std.testing;
const Code = opendal.Code;

test "Opendal BDD test" {
    const c_str = [*:0]const u8; // define a type for 'const char*' in C

    const OpendalBDDTest = struct {
        p: [*c]opendal.c.opendal_operator,
        scheme: c_str,
        path: c_str,
        content: c_str,

        pub fn init() Self {
            var self: Self = undefined;
            self.scheme = "memory";
            self.path = "test";
            self.content = "Hello, World!";

            const options: [*c]opendal.c.opendal_operator_options = opendal.c.opendal_operator_options_new();
            defer opendal.c.opendal_operator_options_free(options);
            opendal.c.opendal_operator_options_set(options, "root", "/myroot");

            // Given A new OpenDAL Blocking Operator
            const result = opendal.c.opendal_operator_new(self.scheme, options);
            testing.expectEqual(result.@"error", null) catch unreachable;
            self.p = result.op;

            return self;
        }

        pub fn deinit(self: *Self) void {
            opendal.c.opendal_operator_free(self.p);
        }

        const Self = @This();
    };

    var testkit = OpendalBDDTest.init();
    defer testkit.deinit();

    const allocator = std.heap.page_allocator;
    const dupe_content = try allocator.dupeZ(u8, std.mem.span(testkit.content));
    // When Blocking write path "test" with content "Hello, World!"
    const data: opendal.c.opendal_bytes = .{
        .data = dupe_content.ptr,
        .len = dupe_content.len,
        .capacity = dupe_content.len,
    };
    const result = opendal.c.opendal_operator_write(testkit.p, testkit.path, &data);
    try testing.expectEqual(result, null);

    // The blocking file "test" should exist
    const e: opendal.c.opendal_result_is_exist = opendal.c.opendal_operator_is_exist(testkit.p, testkit.path);
    try testing.expectEqual(e.@"error", null);
    try testing.expect(e.is_exist);

    // The blocking file "test" entry mode must be file
    const s: opendal.c.opendal_result_stat = opendal.c.opendal_operator_stat(testkit.p, testkit.path);
    try testing.expectEqual(s.@"error", null);
    const meta: [*c]opendal.c.opendal_metadata = s.meta;
    try testing.expect(opendal.c.opendal_metadata_is_file(meta));

    // The blocking file "test" content length must be 13
    try testing.expectEqual(opendal.c.opendal_metadata_content_length(meta), 13);
    defer opendal.c.opendal_metadata_free(meta);

    // The blocking file "test" must have content "Hello, World!"
    var r: opendal.c.opendal_result_read = opendal.c.opendal_operator_read(testkit.p, testkit.path);
    defer opendal.c.opendal_bytes_free(&r.data);
    try testing.expect(r.@"error" == null);
    try testing.expectEqual(std.mem.len(testkit.content), r.data.len);

    var count: usize = 0;
    while (count < r.data.len) : (count += 1) {
        try testing.expectEqual(testkit.content[count], r.data.data[count]);
    }
}

test "Semantic Analyzer" {
    testing.refAllDeclsRecursive(@This());
}
