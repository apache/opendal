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

pub const opendal = @cImport(@cInclude("opendal.h"));

const std = @import("std");
const testing = std.testing;

test "Opendal BDD test" {
    const c_str = [*:0]const u8; // const char*
    // const zig_str = []const u8;

    const OpendalBddTest = struct {
        p: opendal.opendal_operator_ptr,
        scheme: c_str,
        path: c_str,
        content: c_str,

        pub fn init() Self {
            var self: Self = undefined;
            self.scheme = "memory";
            self.path = "test";
            self.content = "Hello, World!";

            var options: opendal.opendal_operator_options = opendal.opendal_operator_options_new();
            opendal.opendal_operator_options_set(&options, "root", "/myroot");

            // Given A new OpenDAL Blocking Operator
            self.p = opendal.opendal_operator_new(self.scheme, options);
            defer opendal.opendal_operator_options_free(&options);
            // try testing.expect(self.p != null);
            return self;
        }
        pub fn deinit(self: *Self) void {
            opendal.opendal_operator_free(&self.p);
        }

        const Self = @This();
    };
    var bddTest = OpendalBddTest.init();
    defer bddTest.deinit();

    // When Blocking write path "test" with content "Hello, World!"
    const data: opendal.opendal_bytes = .{
        .data = bddTest.content,
        // c_str hasn't len field (.* is ptr)
        .len = std.mem.len(bddTest.content),
    };
    const code = opendal.opendal_operator_blocking_write(bddTest.p, bddTest.path, data);
    try testing.expectEqual(code, opendal.OPENDAL_OK);

    // The blocking file "test" should exist
    var e: opendal.opendal_result_is_exist = opendal.opendal_operator_is_exist(bddTest.p, bddTest.path);
    try testing.expectEqual(e.code, opendal.OPENDAL_OK);
    try testing.expect(e.is_exist);

    // The blocking file "test" entry mode must be file
    var s: opendal.opendal_result_stat = opendal.opendal_operator_stat(bddTest.p, bddTest.path);
    try testing.expectEqual(s.code, opendal.OPENDAL_OK);
    var meta: opendal.opendal_metadata = s.meta;
    try testing.expect(opendal.opendal_metadata_is_file(&meta));

    // The blocking file "test" content length must be 13
    try testing.expectEqual(opendal.opendal_metadata_content_length(&meta), 13);
    defer opendal.opendal_metadata_free(&meta);

    // The blocking file "test" must have content "Hello, World!"
    var r: opendal.opendal_result_read = opendal.opendal_operator_blocking_read(bddTest.p, bddTest.path);
    defer opendal.opendal_bytes_free(r.data);
    try testing.expect(r.code == opendal.OPENDAL_OK);
    try testing.expectEqual(std.mem.len(r.data.*.data), std.mem.len(bddTest.content));

    var count: usize = 0;
    while (count < std.mem.len(r.data.*.data)) : (count += 1) {
        try testing.expectEqual(bddTest.content[count], r.data.*.data[count]);
    }
}
