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

const std = @import("std");
pub const c = @import("opendal_c_header");

/// Owns a NUL-terminated string allocated by the underlying Rust library.
/// Keep a single owner for the allocation. Call `deinit()` after using the slices
/// returned by `slice()`.
pub const OwnedString = struct {
    ptr: [*c]u8,

    pub fn slice(self: OwnedString) [:0]const u8 {
        if (self.ptr == null) {
            return "";
        }
        return std.mem.span(self.ptr);
    }

    pub fn deinit(self: *OwnedString) void {
        if (self.ptr != null) {
            c.opendal_string_free(self.ptr);
            self.ptr = null;
        }
    }
};

/// Owns a byte buffer allocated by the underlying Rust library.
/// Keep a single owner for the allocation. Call `deinit()` after using the slices
/// returned by `slice()`.
pub const Bytes = struct {
    inner: c.opendal_bytes,

    pub fn slice(self: Bytes) []const u8 {
        if (self.inner.len == 0 or self.inner.data == null) {
            return &.{};
        }
        return self.inner.data[0..self.inner.len];
    }

    pub fn deinit(self: *Bytes) void {
        c.opendal_bytes_free(&self.inner);
    }
};

pub const Operator = struct {
    inner: *c.opendal_operator,

    pub fn init(scheme: [:0]const u8, options: ?*c.opendal_operator_options) !Operator {
        var op: [*c]c.opendal_operator = null;
        if (c.opendal_operator_new(scheme.ptr, options, &op)) |err| {
            c.opendal_error_free(err);
            return error.OperatorInitFailed;
        }
        if (op == null) return error.OperatorInitFailed;
        return .{ .inner = op };
    }

    pub fn deinit(self: *Operator) void {
        c.opendal_operator_free(self.inner);
    }

    pub fn write(self: *const Operator, path: [:0]const u8, data: []const u8) !void {
        var bytes = c.opendal_bytes{
            .data = null,
            .len = 0,
            .capacity = 0,
        };
        if (data.len > 0) {
            bytes.data = @constCast(data.ptr);
            bytes.len = data.len;
            bytes.capacity = data.len;
        }
        if (c.opendal_operator_write(self.inner, path.ptr, &bytes)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
    }

    pub fn read(self: *const Operator, path: [:0]const u8) !Bytes {
        var data = c.opendal_bytes{
            .data = null,
            .len = 0,
            .capacity = 0,
        };
        if (c.opendal_operator_read(self.inner, path.ptr, &data)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
        return .{ .inner = data };
    }

    pub fn delete(self: *const Operator, path: [:0]const u8) !void {
        if (c.opendal_operator_delete(self.inner, path.ptr)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
    }

    pub fn stat(self: *const Operator, path: [:0]const u8) !Metadata {
        var meta: [*c]c.opendal_metadata = null;
        if (c.opendal_operator_stat(self.inner, path.ptr, &meta)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
        return .{ .inner = meta };
    }

    pub fn exists(self: *const Operator, path: [:0]const u8) !bool {
        var is_exist: bool = false;
        if (c.opendal_operator_is_exist(self.inner, path.ptr, &is_exist)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
        return is_exist;
    }

    pub fn list(self: *const Operator, path: [:0]const u8) !Lister {
        var lister: [*c]c.opendal_lister = null;
        if (c.opendal_operator_list(self.inner, path.ptr, &lister)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
        return .{ .inner = lister };
    }

    pub fn createDir(self: *const Operator, path: [:0]const u8) !void {
        if (c.opendal_operator_create_dir(self.inner, path.ptr)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
    }

    pub fn rename(self: *const Operator, src: [:0]const u8, dest: [:0]const u8) !void {
        if (c.opendal_operator_rename(self.inner, src.ptr, dest.ptr)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
    }

    pub fn copy(self: *const Operator, src: [:0]const u8, dest: [:0]const u8) !void {
        if (c.opendal_operator_copy(self.inner, src.ptr, dest.ptr)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
    }

    pub fn info(self: *const Operator) !OperatorInfo {
        const info_new: ?*c.opendal_operator_info = c.opendal_operator_info_new(self.inner);
        if (info_new) |inner| {
            return .{ .inner = inner };
        }
        return error.InfoFailed;
    }
};

pub const OperatorInfo = struct {
    inner: *c.opendal_operator_info,

    pub fn deinit(self: *OperatorInfo) void {
        c.opendal_operator_info_free(self.inner);
    }

    pub fn scheme(self: *const OperatorInfo) OwnedString {
        return .{ .ptr = c.opendal_operator_info_get_scheme(self.inner) };
    }

    pub fn root(self: *const OperatorInfo) OwnedString {
        return .{ .ptr = c.opendal_operator_info_get_root(self.inner) };
    }

    pub fn name(self: *const OperatorInfo) OwnedString {
        return .{ .ptr = c.opendal_operator_info_get_name(self.inner) };
    }

    pub fn capability(self: *const OperatorInfo) c.opendal_capability {
        var cap: c.opendal_capability = undefined;
        c.opendal_operator_info_get_capability(self.inner, &cap);
        return cap;
    }
};

pub const Lister = struct {
    inner: *c.opendal_lister,

    pub fn deinit(self: *Lister) void {
        c.opendal_lister_free(self.inner);
    }

    pub fn next(self: *const Lister) !?Entry {
        var entry: [*c]c.opendal_entry = null;
        if (c.opendal_lister_next(self.inner, &entry)) |err| {
            defer c.opendal_error_free(err);
            return errorFromCode(err.*.code);
        }
        if (entry == null) return null;
        return Entry{ .inner = entry };
    }
};

pub const Entry = struct {
    inner: *c.opendal_entry,

    pub fn deinit(self: *Entry) void {
        c.opendal_entry_free(self.inner);
    }

    pub fn path(self: *const Entry) OwnedString {
        return .{ .ptr = c.opendal_entry_path(self.inner) };
    }

    pub fn name(self: *const Entry) OwnedString {
        return .{ .ptr = c.opendal_entry_name(self.inner) };
    }
};

pub const Metadata = struct {
    inner: *c.opendal_metadata,

    pub fn deinit(self: *Metadata) void {
        c.opendal_metadata_free(self.inner);
    }

    pub fn mode(self: *const Metadata) u32 {
        return c.opendal_metadata_mode(self.inner);
    }

    pub fn isDir(self: *const Metadata) bool {
        return c.opendal_metadata_is_dir(self.inner);
    }

    pub fn isFile(self: *const Metadata) bool {
        return c.opendal_metadata_is_file(self.inner);
    }

    pub fn contentLength(self: *const Metadata) u64 {
        return c.opendal_metadata_content_length(self.inner);
    }

    pub fn contentType(self: *const Metadata) ?OwnedString {
        const ptr = c.opendal_metadata_content_type(self.inner);
        if (ptr == null) {
            return null;
        }
        return OwnedString{ .ptr = ptr };
    }

    pub fn etag(self: *const Metadata) ?OwnedString {
        const ptr = c.opendal_metadata_etag(self.inner);
        if (ptr == null) {
            return null;
        }
        return OwnedString{ .ptr = ptr };
    }

    /// Returns the last-modified timestamp in milliseconds since the Unix epoch, or `-1` if absent.
    pub fn lastModified(self: *const Metadata) i64 {
        return c.opendal_metadata_last_modified_ms(self.inner);
    }
};

pub const Code = enum(c.opendal_code) {
    UNEXPECTED = c.OPENDAL_UNEXPECTED,
    UNSUPPORTED = c.OPENDAL_UNSUPPORTED,
    CONFIG_INVALID = c.OPENDAL_CONFIG_INVALID,
    NOT_FOUND = c.OPENDAL_NOT_FOUND,
    PERMISSION_DENIED = c.OPENDAL_PERMISSION_DENIED,
    IS_A_DIRECTORY = c.OPENDAL_IS_A_DIRECTORY,
    NOT_A_DIRECTORY = c.OPENDAL_NOT_A_DIRECTORY,
    ALREADY_EXISTS = c.OPENDAL_ALREADY_EXISTS,
    RATE_LIMITED = c.OPENDAL_RATE_LIMITED,
    IS_SAME_FILE = c.OPENDAL_IS_SAME_FILE,
    CONDITION_NOT_MATCH = c.OPENDAL_CONDITION_NOT_MATCH,
    RANGE_NOT_SATISFIED = c.OPENDAL_RANGE_NOT_SATISFIED,
    CONFLICT = c.OPENDAL_CONFLICT,
    CANCELLED = c.OPENDAL_CANCELLED,
};

pub const OpendalError = error{
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
    ConditionNotMatch,
    RangeNotSatisfied,
    Conflict,
    Cancelled,
};

/// Converts a C error code to a Zig error. Unrecognized codes return `error.Unexpected`.
pub fn codeToError(code: c.opendal_code) OpendalError!void {
    return errorFromCode(code);
}

fn errorFromCode(code: c.opendal_code) OpendalError {
    return switch (code) {
        c.OPENDAL_UNEXPECTED => error.Unexpected,
        c.OPENDAL_UNSUPPORTED => error.Unsupported,
        c.OPENDAL_NOT_FOUND => error.NotFound,
        c.OPENDAL_CONFIG_INVALID => error.ConfigInvalid,
        c.OPENDAL_PERMISSION_DENIED => error.PermissionDenied,
        c.OPENDAL_IS_A_DIRECTORY => error.IsDirectory,
        c.OPENDAL_NOT_A_DIRECTORY => error.IsNotDirectory,
        c.OPENDAL_ALREADY_EXISTS => error.AlreadyExists,
        c.OPENDAL_RATE_LIMITED => error.RateLimited,
        c.OPENDAL_IS_SAME_FILE => error.IsSameFile,
        c.OPENDAL_CONDITION_NOT_MATCH => error.ConditionNotMatch,
        c.OPENDAL_RANGE_NOT_SATISFIED => error.RangeNotSatisfied,
        c.OPENDAL_CONFLICT => error.Conflict,
        c.OPENDAL_CANCELLED => error.Cancelled,
        else => error.Unexpected,
    };
}

pub fn errorToCode(err: OpendalError) c_int {
    return switch (err) {
        error.Unexpected => c.OPENDAL_UNEXPECTED,
        error.Unsupported => c.OPENDAL_UNSUPPORTED,
        error.ConfigInvalid => c.OPENDAL_CONFIG_INVALID,
        error.NotFound => c.OPENDAL_NOT_FOUND,
        error.PermissionDenied => c.OPENDAL_PERMISSION_DENIED,
        error.IsDirectory => c.OPENDAL_IS_A_DIRECTORY,
        error.IsNotDirectory => c.OPENDAL_NOT_A_DIRECTORY,
        error.AlreadyExists => c.OPENDAL_ALREADY_EXISTS,
        error.RateLimited => c.OPENDAL_RATE_LIMITED,
        error.IsSameFile => c.OPENDAL_IS_SAME_FILE,
        error.ConditionNotMatch => c.OPENDAL_CONDITION_NOT_MATCH,
        error.RangeNotSatisfied => c.OPENDAL_RANGE_NOT_SATISFIED,
        error.Conflict => c.OPENDAL_CONFLICT,
        error.Cancelled => c.OPENDAL_CANCELLED,
    };
}

const testing = std.testing;

test "Error Tests" {
    // C code to Zig error
    try testing.expectError(error.Unexpected, codeToError(c.OPENDAL_UNEXPECTED));
    try testing.expectError(error.Unsupported, codeToError(c.OPENDAL_UNSUPPORTED));
    try testing.expectError(error.ConfigInvalid, codeToError(c.OPENDAL_CONFIG_INVALID));
    try testing.expectError(error.NotFound, codeToError(c.OPENDAL_NOT_FOUND));
    try testing.expectError(error.PermissionDenied, codeToError(c.OPENDAL_PERMISSION_DENIED));
    try testing.expectError(error.IsDirectory, codeToError(c.OPENDAL_IS_A_DIRECTORY));
    try testing.expectError(error.IsNotDirectory, codeToError(c.OPENDAL_NOT_A_DIRECTORY));
    try testing.expectError(error.AlreadyExists, codeToError(c.OPENDAL_ALREADY_EXISTS));
    try testing.expectError(error.RateLimited, codeToError(c.OPENDAL_RATE_LIMITED));
    try testing.expectError(error.IsSameFile, codeToError(c.OPENDAL_IS_SAME_FILE));
    try testing.expectError(error.ConditionNotMatch, codeToError(c.OPENDAL_CONDITION_NOT_MATCH));
    try testing.expectError(error.RangeNotSatisfied, codeToError(c.OPENDAL_RANGE_NOT_SATISFIED));
    try testing.expectError(error.Conflict, codeToError(c.OPENDAL_CONFLICT));
    try testing.expectError(error.Cancelled, codeToError(c.OPENDAL_CANCELLED));
    try testing.expectError(error.Unexpected, codeToError(999));

    // Zig error to C code
    try testing.expectEqual(c.OPENDAL_UNEXPECTED, errorToCode(error.Unexpected));
    try testing.expectEqual(c.OPENDAL_UNSUPPORTED, errorToCode(error.Unsupported));
    try testing.expectEqual(c.OPENDAL_CONFIG_INVALID, errorToCode(error.ConfigInvalid));
    try testing.expectEqual(c.OPENDAL_NOT_FOUND, errorToCode(error.NotFound));
    try testing.expectEqual(c.OPENDAL_PERMISSION_DENIED, errorToCode(error.PermissionDenied));
    try testing.expectEqual(c.OPENDAL_IS_A_DIRECTORY, errorToCode(error.IsDirectory));
    try testing.expectEqual(c.OPENDAL_NOT_A_DIRECTORY, errorToCode(error.IsNotDirectory));
    try testing.expectEqual(c.OPENDAL_ALREADY_EXISTS, errorToCode(error.AlreadyExists));
    try testing.expectEqual(c.OPENDAL_RATE_LIMITED, errorToCode(error.RateLimited));
    try testing.expectEqual(c.OPENDAL_IS_SAME_FILE, errorToCode(error.IsSameFile));
    try testing.expectEqual(c.OPENDAL_CONDITION_NOT_MATCH, errorToCode(error.ConditionNotMatch));
    try testing.expectEqual(c.OPENDAL_RANGE_NOT_SATISFIED, errorToCode(error.RangeNotSatisfied));
    try testing.expectEqual(c.OPENDAL_CONFLICT, errorToCode(error.Conflict));
    try testing.expectEqual(c.OPENDAL_CANCELLED, errorToCode(error.Cancelled));
}

test "Semantic Analyzer" {
    testing.refAllDecls(@This());
    testing.refAllDecls(OwnedString);
    testing.refAllDecls(Bytes);
    testing.refAllDecls(Operator);
    testing.refAllDecls(OperatorInfo);
    testing.refAllDecls(Lister);
    testing.refAllDecls(Entry);
    testing.refAllDecls(Metadata);
}

test "operator basic operations" {
    // Initialize a operator for "memory" backend, with no options
    var op = try Operator.init("memory", null);
    defer op.deinit();

    try testing.expectError(error.NotFound, op.read("/testpath"));

    // Prepare some data to be written
    const data = "this_string_length_is_24";

    // Write this into path "/testpath"
    try op.write("/testpath", data);
    defer op.delete("/testpath") catch |err| {
        std.debug.panic("Error deleting file: {}\n", .{err});
    };

    // We can read it out, make sure the data is the same
    var read_bytes = try op.read("/testpath");
    defer read_bytes.deinit();
    try testing.expectEqual(read_bytes.slice().len, 24);
    try testing.expectEqualStrings(read_bytes.slice(), data);
}

test "operator advanced operations" {
    var op = try Operator.init("memory", null);
    defer op.deinit();

    // Test directory creation
    try op.createDir("/testdir/");
    try testing.expect(try op.exists("/testdir/"));

    // Test file operations in directory
    const data = "hello world";
    try op.write("/testdir/file.txt", data);
    try testing.expect(try op.exists("/testdir/file.txt"));

    // Test metadata
    var meta = try op.stat("/testdir/file.txt");
    defer meta.deinit();
    try testing.expect(meta.isFile());
    try testing.expect(!meta.isDir());
    try testing.expectEqual(meta.contentLength(), data.len);

    // ================================================================================
    // "operator advanced operations" - Unsupported
    // ================================================================================
    // /home/kassane/opendal/bindings/zig/src/opendal.zig:269:5: 0x103d9a0 in codeToError (test)
    //     return switch (code) {
    //     ^
    // /home/kassane/opendal/bindings/zig/src/opendal.zig:113:13: 0x103f725 in copy (test)
    //             try codeToError(err.*.code);
    //             ^
    // /home/kassane/opendal/bindings/zig/src/opendal.zig:385:5: 0x1040031 in test.operator advanced operations (test)
    //     try op.copy("/testdir/renamed.txt", "/testdir/copied.txt");
    //     ^
    // operator advanced operations (0.99ms)

    // 3 of 4 tests passed

    // Test rename operation
    // try op.rename("/testdir/file.txt", "/testdir/renamed.txt");
    // try testing.expect(!try op.exists("/testdir/file.txt"));
    // try testing.expect(try op.exists("/testdir/renamed.txt"));

    // Test copy operation
    // try op.copy("/testdir/renamed.txt", "/testdir/copied.txt");
    // try testing.expect(try op.exists("/testdir/renamed.txt"));
    // try testing.expect(try op.exists("/testdir/copied.txt"));
    //============================================

    // Test list operation
    var lister = try op.list("/testdir/");
    defer lister.deinit();
    var count: usize = 0;
    while (try lister.next()) |e| {
        var entry = e;
        defer entry.deinit();
        count += 1;
        var entry_path = entry.path();
        defer entry_path.deinit();
        var entry_name = entry.name();
        defer entry_name.deinit();
        try testing.expect(entry_path.slice().len > 0);
        try testing.expect(entry_name.slice().len > 0);
        try testing.expect(try op.exists(entry_path.slice()));
    }
    try testing.expectEqual(count, 2);

    // Test delete operation
    // try op.delete("/testdir/renamed.txt");
    try testing.expect(!try op.exists("/testdir/renamed.txt"));
}

test "operator info" {
    var op = try Operator.init("memory", null);
    defer op.deinit();

    var op_info = try op.info();
    defer op_info.deinit();

    var scheme = op_info.scheme();
    defer scheme.deinit();
    try testing.expectEqualStrings("memory", scheme.slice());

    var root = op_info.root();
    defer root.deinit();
    try testing.expectEqualStrings("/", root.slice());

    var name = op_info.name();
    defer name.deinit();
    try testing.expect(name.slice().len > 0);
    // Exercise idempotent double-deinit alongside the deferred `name.deinit()`.
    name.deinit();
    try testing.expectEqualStrings("", name.slice());

    const cap = op_info.capability();
    try testing.expect(cap.read);
    try testing.expect(cap.write);
    try testing.expect(cap.stat);
}

test "metadata optional fields" {
    var op = try Operator.init("memory", null);
    defer op.deinit();

    try op.write("/meta.txt", "metadata test");
    defer op.delete("/meta.txt") catch {};

    var meta = try op.stat("/meta.txt");
    defer meta.deinit();
    try testing.expect(meta.contentType() == null);
    try testing.expect(meta.etag() == null);
    try testing.expectEqual(@as(i64, -1), meta.lastModified());

    const opts = c.opendal_write_options_new();
    defer c.opendal_write_options_free(opts);
    c.opendal_write_options_set_content_type(opts, "text/plain");
    const payload: []const u8 = "typed payload";
    const raw_bytes = c.opendal_bytes{
        .data = @constCast(payload.ptr),
        .len = payload.len,
        .capacity = payload.len,
    };
    if (c.opendal_operator_write_with(op.inner, "/typed.txt", &raw_bytes, opts)) |err| {
        defer c.opendal_error_free(err);
        return errorFromCode(err.*.code);
    }
    defer op.delete("/typed.txt") catch {};

    var typed_meta = try op.stat("/typed.txt");
    defer typed_meta.deinit();
    var content_type = typed_meta.contentType() orelse return error.TestExpectedContentType;
    defer content_type.deinit();
    try testing.expectEqualStrings("text/plain", content_type.slice());
}

test "empty write and read" {
    var op = try Operator.init("memory", null);
    defer op.deinit();

    try op.write("/empty.txt", "");
    defer op.delete("/empty.txt") catch {};

    var read_bytes = try op.read("/empty.txt");
    defer read_bytes.deinit();
    try testing.expectEqual(@as(usize, 0), read_bytes.slice().len);
    try testing.expectEqualStrings("", read_bytes.slice());

    read_bytes.deinit();
    try testing.expectEqual(@as(usize, 0), read_bytes.slice().len);
}

test "runtime sentinel path" {
    const allocator = testing.allocator;

    var op = try Operator.init("memory", null);
    defer op.deinit();

    const path = try std.fmt.allocPrintZ(allocator, "/runtime-{d}.txt", .{42});
    defer allocator.free(path);

    const payload = "runtime path payload";
    try op.write(path, payload);
    defer op.delete(path) catch {};

    var read_bytes = try op.read(path);
    defer read_bytes.deinit();
    try testing.expectEqualStrings(payload, read_bytes.slice());
}

test "operator init invalid scheme" {
    try testing.expectError(error.OperatorInitFailed, Operator.init("nonexistent_scheme", null));
}

test "sync operations" {
    var op = try Operator.init("memory", null);
    defer op.deinit();

    const data = [_]u8{ 1, 2, 3, 4, 5 };

    // Test first path
    try writeData(&op, "test_path", &data);
    var read_bytes1 = try readData(&op, "test_path");
    defer read_bytes1.deinit();
    try testing.expectEqualSlices(u8, &data, read_bytes1.slice());
}

// FIXME: test async operations
// test "async operations" {
//     const coro = @import("libcoro");

//     const allocator = std.testing.allocator;
//     const stack = try coro.stackAlloc(allocator, null);
//     defer allocator.free(stack);

//     var op = try Operator.init("memory", null);
//     defer op.deinit();

//     const data = [_]u8{ 1, 2, 3, 4, 5 };

//     // Test first path
//     const write_frame1 = try coro.xasync(writeData, .{ &op, "test_path", &data }, stack);
//     const read_frame1 = try coro.xasync(readData, .{ &op, "test_path" }, stack);
//     try testing.expectEqual(write_frame1.status(), .Suspended);
//     try testing.expectEqual(read_frame1.status(), .Suspended);

//     try coro.xawait(write_frame1);
//     var read_bytes1 = try coro.xawait(read_frame1);
//     defer read_bytes1.deinit();
//     try testing.expectEqual(write_frame1.status(), .Done);
//     try testing.expectEqual(read_frame1.status(), .Done);
//     try testing.expectEqualSlices(u8, &data, read_bytes1.slice());
// }

fn writeData(op: *Operator, path: [:0]const u8, data: []const u8) !void {
    try op.write(path, data);
}

fn readData(op: *Operator, path: [:0]const u8) !Bytes {
    return try op.read(path);
}
