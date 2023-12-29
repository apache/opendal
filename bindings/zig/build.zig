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

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // This function creates a module and adds it to the package's module set, making
    // it available to other packages which depend on this one.
    _ = b.addModule("opendal", .{
        .source_file = .{
            .path = "src/opendal.zig",
        },
        .dependencies = &.{},
    });

    // Creates a step for building the dependent C bindings
    const libopendal_c = buildLibOpenDAL(b);
    const build_libopendal_c = b.step("libopendal_c", "Build OpenDAL C bindings");
    build_libopendal_c.dependOn(&libopendal_c.step);

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const unit_tests = b.addTest(.{
        .root_source_file = .{
            .path = "test/bdd.zig",
        },
        .target = target,
        .optimize = optimize,
    });
    unit_tests.addIncludePath(.{ .path = "../c/include" });
    unit_tests.addModule("opendal", module(b));
    if (optimize == .Debug) {
        unit_tests.addLibraryPath(.{ .path = "../c/target/debug" });
    } else {
        unit_tests.addLibraryPath(.{ .path = "../c/target/release" });
    }
    unit_tests.linkSystemLibrary("opendal_c");
    unit_tests.linkLibCpp();

    // Creates a step for running unit tests.
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run OpenDAL Zig bindings tests");
    test_step.dependOn(&libopendal_c.step);
    test_step.dependOn(&run_unit_tests.step);
}

fn buildLibOpenDAL(b: *std.Build) *std.Build.Step.Run {
    const basedir = comptime std.fs.path.dirname(@src().file) orelse null;
    const c_bindings_dir = basedir ++ "/../c";
    return b.addSystemCommand(&[_][]const u8{
        "make",
        "-C",
        c_bindings_dir,
        "build",
    });
}

pub fn module(b: *std.Build) *std.Build.Module {
    return b.createModule(.{
        .source_file = .{
            .path = "src/opendal.zig",
        },
    });
}
