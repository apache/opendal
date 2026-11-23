# Upgrade Guide

## Unreleased

### Breaking Changes

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
