---
title: Getting started
sidebar_label: Getting started
description: Build an operator, write and read data, inspect metadata, and list a directory with the OpenDAL Zig binding.
---

# Getting started

Before running anything, make sure you have built the binding:

```shell
# From bindings/zig/
zig build libopendal_c
```

## Your first program

The example below initializes an operator backed by the in-memory service, writes a
file, reads it back, checks its metadata, and deletes it. It needs no credentials and
no external service.

```zig file=bindings/zig/examples/getting_started.zig region=quickstart
```

`Operator.init` takes a NUL-terminated service scheme and an optional pointer to
an `opendal_operator_options` struct. Passing `null` uses defaults.

`read` returns a `Bytes` owner. Use `slice()` to borrow the data, then call
`deinit()` when you are done. Keep a single owner for each allocation.

## Runtime paths and schemes

Scheme and path parameters use `[:0]const u8`. String literals work directly.
Use UTF-8 text without embedded NUL bytes. This example copies a scheme
name from a `[]const u8` with `allocator.dupeZ` and formats a path from `id`
with `std.fmt.allocPrintZ`. Free both strings with the allocator that created
them.

```zig
const scheme = try allocator.dupeZ(u8, scheme_name);
defer allocator.free(scheme);
var op = try opendal.Operator.init(scheme, null);
defer op.deinit();

const path = try std.fmt.allocPrintZ(allocator, "/object-{d}.txt", .{id});
defer allocator.free(path);
try op.write(path, "Hello, World!");
var data = try op.read(path);
defer data.deinit();
std.debug.print("{s}\n", .{data.slice()});
```

## Listing a directory

`list` returns a `Lister`. Call `next` in a loop until it returns `null`.

```zig
var op = try opendal.Operator.init("memory", null);
defer op.deinit();

try op.createDir("/testdir/");
try op.write("/testdir/a.txt", "aaa");
try op.write("/testdir/b.txt", "bbb");

var lister = try op.list("/testdir/");
defer lister.deinit();

while (try lister.next()) |entry| {
    var e = entry;
    defer e.deinit();
    var name = e.name();
    defer name.deinit();
    var path = e.path();
    defer path.deinit();
    std.debug.print("name={s}  path={s}\n", .{ name.slice(), path.slice() });
}
```

`Entry.name()` and `Entry.path()` return `OwnedString` values. Keep each owner
alive while using its slice. You can pass `path.slice()` directly to operations
such as `read` or `stat`. The same ownership rules apply to strings from
`OperatorInfo` and to any value returned by `contentType()` or `etag()`.

## Passing service options

Service-specific configuration is set through the C API's `opendal_operator_options`.
The Zig binding re-exports the raw C namespace as `opendal.c`:

```zig
const c = opendal.c;

const options = c.opendal_operator_options_new();
defer c.opendal_operator_options_free(options);
c.opendal_operator_options_set(options, "root", "/myroot");

// opendal_operator_options_new() returns [*c]..., but Operator.init expects ?*.
// @ptrCast converts the many-item C pointer to a single-item optional pointer.
var op = try opendal.Operator.init("memory", @ptrCast(options));
defer op.deinit();
```

## Error handling

`Operator.init` returns `error.OperatorInitFailed` if it cannot construct the
operator. Storage operations map C error codes to named Zig errors in the
`OpendalError` set. The table below shows common variants. The full set, including
`error.IsDirectory`, `error.IsNotDirectory`, `error.RateLimited`,
`error.IsSameFile`, `error.ConditionNotMatch`, and `error.RangeNotSatisfied`, is
defined in `src/opendal.zig`.

| Zig error | Meaning |
|---|---|
| `error.NotFound` | Path does not exist |
| `error.PermissionDenied` | Access denied |
| `error.AlreadyExists` | Path already exists |
| `error.Unsupported` | Backend does not support the operation |
| `error.ConfigInvalid` | Invalid backend or operation configuration |
| `error.Unexpected` | Unclassified error from the backend |

Handle them with a standard `catch` or `try`:

```zig
var data = op.read("/missing.txt") catch |err| {
    if (err == error.NotFound) {
        std.debug.print("file not found\n", .{});
        return;
    }
    return err;
};
defer data.deinit();
```

## Running the binding's own tests

```shell
zig build test --summary all
```

All tests run against the in-memory backend; no credentials are required.
