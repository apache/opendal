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

// Zig code get values C code (same values)
const Code = enum(opendal.opendal_code) {
    OK = opendal.OPENDAL_OK,
    ERROR = opendal.OPENDAL_ERROR,
    UNEXPECTED = opendal.OPENDAL_UNEXPECTED,
    UNSUPPORTED = opendal.OPENDAL_UNSUPPORTED,
    CONFIG_INVALID = opendal.OPENDAL_CONFIG_INVALID,
    NOT_FOUND = opendal.OPENDAL_NOT_FOUND,
    PERMISSION_DENIED = opendal.OPENDAL_PERMISSION_DENIED,
    IS_A_DIRECTORY = opendal.OPENDAL_IS_A_DIRECTORY,
    NOT_A_DIRECTORY = opendal.OPENDAL_NOT_A_DIRECTORY,
    ALREADY_EXISTS = opendal.OPENDAL_ALREADY_EXISTS,
    RATE_LIMITED = opendal.OPENDAL_RATE_LIMITED,
    IS_SAME_FILE = opendal.OPENDAL_IS_SAME_FILE,
};

const OpendalError = error{
    Unexpected,
    Unsupported,
    ConfigInvalid,
    NotFound,
    PermissionDenied,
    IsDirectory,
    IsNotDirectory,
    AlreadyExists,
    RateLimited,
    IsSameFile,
};

pub fn errorsToZig(code: opendal.opendal_code) OpendalError!opendal.opendal_code {
    return switch (code) {
        opendal.OPENDAL_UNEXPECTED => error.Unexpected,
        opendal.OPENDAL_UNSUPPORTED => error.Unsupported,
        opendal.OPENDAL_NOT_FOUND => error.NotFound,
        opendal.OPENDAL_CONFIG_INVALID => error.ConfigInvalid,
        opendal.OPENDAL_PERMISSION_DENIED => error.PermissionDenied,
        opendal.OPENDAL_IS_A_DIRECTORY => error.IsDirectory,
        opendal.OPENDAL_NOT_A_DIRECTORY => error.IsNotDirectory,
        opendal.OPENDAL_ALREADY_EXISTS => error.AlreadyExists,
        opendal.OPENDAL_RATE_LIMITED => error.RateLimited,
        opendal.OPENDAL_IS_SAME_FILE => error.IsSameFile,
        else => opendal.OPENDAL_ERROR,
    };
}
pub fn errorsFromZig(err: OpendalError) OpendalError!c_int {
    return switch (err) {
        error.Unexpected => opendal.OPENDAL_UNEXPECTED,
        error.Unsupported => opendal.OPENDAL_UNSUPPORTED,
        error.ConfigInvalid => opendal.OPENDAL_CONFIG_INVALID,
        error.NotFound => opendal.OPENDAL_NOT_FOUND,
        error.PermissionDenied => opendal.OPENDAL_PERMISSION_DENIED,
        error.IsDirectory => opendal.OPENDAL_IS_A_DIRECTORY,
        error.IsNotDirectory => opendal.OPENDAL_NOT_A_DIRECTORY,
        error.AlreadyExists => opendal.OPENDAL_ALREADY_EXISTS,
        error.RateLimited => opendal.OPENDAL_RATE_LIMITED,
        error.IsSameFile => opendal.OPENDAL_IS_SAME_FILE,
    };
}

const std = @import("std");
const testing = std.testing;

test "Semantic Analyzer" {
    testing.refAllDecls(@This());
}

test "Error Tests" {
    // C code to Zig error
    try testing.expectError(error.Unexpected, errorsToZig(opendal.OPENDAL_UNEXPECTED));
    try testing.expectError(error.Unsupported, errorsToZig(opendal.OPENDAL_UNSUPPORTED));
    try testing.expectError(error.ConfigInvalid, errorsToZig(opendal.OPENDAL_CONFIG_INVALID));
    try testing.expectError(error.NotFound, errorsToZig(opendal.OPENDAL_NOT_FOUND));
    try testing.expectError(error.PermissionDenied, errorsToZig(opendal.OPENDAL_PERMISSION_DENIED));
    try testing.expectError(error.IsDirectory, errorsToZig(opendal.OPENDAL_IS_A_DIRECTORY));
    try testing.expectError(error.IsNotDirectory, errorsToZig(opendal.OPENDAL_NOT_A_DIRECTORY));
    try testing.expectError(error.AlreadyExists, errorsToZig(opendal.OPENDAL_ALREADY_EXISTS));
    try testing.expectError(error.RateLimited, errorsToZig(opendal.OPENDAL_RATE_LIMITED));
    try testing.expectError(error.IsSameFile, errorsToZig(opendal.OPENDAL_IS_SAME_FILE));

    // Zig error to C code
    try testing.expectEqual(opendal.OPENDAL_UNEXPECTED, try errorsFromZig(error.Unexpected));
    try testing.expectEqual(opendal.OPENDAL_UNSUPPORTED, try errorsFromZig(error.Unsupported));
    try testing.expectEqual(opendal.OPENDAL_CONFIG_INVALID, try errorsFromZig(error.ConfigInvalid));
    try testing.expectEqual(opendal.OPENDAL_NOT_FOUND, try errorsFromZig(error.NotFound));
    try testing.expectEqual(opendal.OPENDAL_PERMISSION_DENIED, try errorsFromZig(error.PermissionDenied));
    try testing.expectEqual(opendal.OPENDAL_IS_A_DIRECTORY, try errorsFromZig(error.IsDirectory));
    try testing.expectEqual(opendal.OPENDAL_NOT_A_DIRECTORY, try errorsFromZig(error.IsNotDirectory));
    try testing.expectEqual(opendal.OPENDAL_ALREADY_EXISTS, try errorsFromZig(error.AlreadyExists));
    try testing.expectEqual(opendal.OPENDAL_RATE_LIMITED, try errorsFromZig(error.RateLimited));
    try testing.expectEqual(opendal.OPENDAL_IS_SAME_FILE, try errorsFromZig(error.IsSameFile));
}

test "Opendal BDD test" {
    const c_str = [*:0]const u8; // define a type for 'const char*' in C

    const OpendalBDDTest = struct {
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
            defer opendal.opendal_operator_options_free(&options);
            opendal.opendal_operator_options_set(&options, "root", "/myroot");

            // Given A new OpenDAL Blocking Operator
            self.p = opendal.opendal_operator_new(self.scheme, &options);
            std.debug.assert(self.p.ptr != null);

            return self;
        }

        pub fn deinit(self: *Self) void {
            opendal.opendal_operator_free(&self.p);
        }

        const Self = @This();
    };

    var testkit = OpendalBDDTest.init();
    defer testkit.deinit();

    // When Blocking write path "test" with content "Hello, World!"
    const data: opendal.opendal_bytes = .{
        .data = testkit.content,
        // c_str does not have len field (.* is ptr)
        .len = std.mem.len(testkit.content),
    };
    const code = opendal.opendal_operator_blocking_write(testkit.p, testkit.path, data);
    try testing.expectEqual(code, @enumToInt(Code.OK));

    // The blocking file "test" should exist
    var e: opendal.opendal_result_is_exist = opendal.opendal_operator_is_exist(testkit.p, testkit.path);
    try testing.expectEqual(e.code, @enumToInt(Code.OK));
    try testing.expect(e.is_exist);

    // The blocking file "test" entry mode must be file
    var s: opendal.opendal_result_stat = opendal.opendal_operator_stat(testkit.p, testkit.path);
    try testing.expectEqual(s.code, @enumToInt(Code.OK));
    var meta: opendal.opendal_metadata = s.meta;
    try testing.expect(opendal.opendal_metadata_is_file(&meta));

    // The blocking file "test" content length must be 13
    try testing.expectEqual(opendal.opendal_metadata_content_length(&meta), 13);
    defer opendal.opendal_metadata_free(&meta);

    // The blocking file "test" must have content "Hello, World!"
    var r: opendal.opendal_result_read = opendal.opendal_operator_blocking_read(testkit.p, testkit.path);
    defer opendal.opendal_bytes_free(r.data);
    try testing.expect(r.code == @enumToInt(Code.OK));
    try testing.expectEqual(std.mem.len(testkit.content), r.data.*.len);

    var count: usize = 0;
    while (count < r.data.*.len) : (count += 1) {
        try testing.expectEqual(testkit.content[count], r.data.*.data[count]);
    }
}
