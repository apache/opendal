# Upgrade Guide

## Unreleased

### Breaking Changes

#### `opendal_capability` layout and `opendal-go-services` companions

The Go binding mirrors the native `opendal_capability` struct and reads it
through `opendal_operator_info_get_capability`. In this release the struct
appends new fields after the released ones and grows in size, and the function
writes it into caller-provided storage instead of returning it by value.
Existing `Capability` methods keep their names and signatures. See the
[C upgrade guide](../c/upgrade.md#opendal_capability-abi-change) for the
layout change.

**Migration:**

Upgrade the Go binding and every `opendal-go-services` companion module you use
together. Each companion ships a `libopendal_c` that must match the ABI expected
by the Go binding. If you build the library yourself, use the matching OpenDAL
sources. Mixing old and new layouts is unsupported.

Later releases can append fields and grow `opendal_capability` again. The Go
binding allocates the struct from its own mirror, so a companion `libopendal_c`
that is newer than the binding may write past it. Upgrade the binding and its
companions together in every release.

#### `context.Context` parameter on I/O operations

All `Operator` I/O methods and non-stdlib handle methods now accept `context.Context` as their first parameter to support cooperative cancellation and deadlines via `opendal_cancel_token`:

- `Operator`: `Read`, `Reader`, `Write`, `Writer`, `CreateDir`, `Delete`, `Stat`, `IsExist`, `List`, `Copy`, `Rename`, `Check`, `Copier`, `Deleter`, `WithDeleter`, `Remove`, `PresignRead`, `PresignWrite`, `PresignDelete`, `PresignStat`
- `Writer`: `Close(ctx)`
- `Lister`: `Next(ctx)`
- `Copier`: `Next(ctx)`, `Abort(ctx)`
- `Deleter`: `Delete(ctx, path, opts...)`, `Flush(ctx)`, `Close(ctx)`

Standard Go `io` interface methods keep their existing signatures (`Reader.Read`, `Reader.Seek`, `Reader.WriteTo`, `Reader.Close`, `Writer.Write`, `Writer.ReadFrom`, `Lister.Close`, `Copier.Close`). Use the new `Reader.ReadContext`, `Reader.SeekContext`, and `Writer.WriteContext` methods when you need per-call cancellation on streaming reads, seeks, or writes.

Cancellation returns an `*opendal.Error` with `CodeCancelled` whose `Unwrap()` returns `context.Canceled` or `context.DeadlineExceeded`, matching both `errors.Is(err, context.Canceled)` / `errors.Is(err, context.DeadlineExceeded)` and `errors.As(err, &opErr)`.

**Migration:**

Pass a `context.Context` (such as your request context or `context.Background()`) as the first argument to each updated method:

```go
// Before
_, err := op.Write("hello.txt", []byte("Hello, OpenDAL!"))
data, err := op.Read("hello.txt")
for lister.Next() { ... }
_, err = w.Close()

// After
_, err := op.Write(ctx, "hello.txt", []byte("Hello, OpenDAL!"))
data, err := op.Read(ctx, "hello.txt")
for lister.Next(ctx) { ... }
_, err = w.Close(ctx)
```

Because the Go binding now calls the `opendal_cancel_token_*` and `*_with_cancel` C ABI symbols, rebuild or upgrade your `opendal-go-services` companion modules against an `opendal-c` version that includes these symbols.
