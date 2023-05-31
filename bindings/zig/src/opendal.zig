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

pub const c = @cImport(@cInclude("opendal.h"));

// Zig code get values C code
pub const Code = enum(c.opendal_code) {
    OK = c.OPENDAL_OK,
    ERROR = c.OPENDAL_ERROR,
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
};

pub fn codeToError(code: c.opendal_code) OpendalError!c.opendal_code {
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
        else => c.OPENDAL_ERROR,
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
    };
}

const std = @import("std");
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
}

test "Semantic Analyzer" {
    testing.refAllDecls(@This());
}
