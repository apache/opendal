# Apache OpenDAL™ Go Binding

[![](https://img.shields.io/badge/status-released-blue)](https://pkg.go.dev/github.com/apache/opendal/bindings/go)
[![Go Reference](https://pkg.go.dev/badge/github.com/apache/opendal/bindings/go.svg)](https://pkg.go.dev/github.com/apache/opendal/bindings/go)

opendal-go is a **Native** Go binding for Apache OpenDAL. It is built on top of `opendal-c` without CGO enabled, using [purego](https://github.com/ebitengine/purego) directly (no `libffi` required).

We release the OpenDAL Go binding independently of the
[`opendal` crate](https://crates.io/crates/opendal) (Rust core). For updates
and compatibility, use the Go binding version instead of the `opendal` crate
version.

## Useful Links

- [User guide](https://opendal.apache.org/docs/bindings/go)
- [API reference](https://pkg.go.dev/github.com/apache/opendal/bindings/go)
- [Services & configuration](https://opendal.apache.org/services)
- [Upgrade guide](./upgrade.md)

## Installation

```bash
go get github.com/apache/opendal/bindings/go@latest
```

Each service is a separate companion module that embeds a prebuilt `opendal-c` shared library. The companion module must match the `opendal-c` ABI expected by your `bindings/go` version (when using unreleased `main` with the pointer/output-parameter C ABI, build the companion service library from source as shown in [CONTRIBUTING.md](CONTRIBUTING.md) until a matching `opendal-go-services` release is published):

```bash
go get github.com/apache/opendal-go-services/memory
```

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/opendal-go-services/memory"
	opendal "github.com/apache/opendal/bindings/go"
)

func main() {
	op, err := opendal.NewOperator(memory.Scheme, opendal.OperatorOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer op.Close()

	ctx := context.Background()

	if _, err := op.Write(ctx, "hello.txt", []byte("Hello, OpenDAL!")); err != nil {
		log.Fatal(err)
	}

	data, err := op.Read(ctx, "hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read content: %s\n", data)
}
```

## Context Cancellation

All `Operator` I/O methods and non-stdlib handle methods (`Lister.Next`, `Copier.Next`, `Copier.Abort`, `Writer.Close`, `Deleter.Delete`, `Deleter.Flush`, `Deleter.Close`) take `context.Context` as their first parameter. `Reader` and `Writer` keep their standard `io.ReadSeekCloser`, `io.WriterTo`, `io.Writer`, and `io.ReaderFrom` signatures and provide `ReadContext`, `SeekContext`, and `WriteContext` for per-call cancellation.

When a context is cancelled or its deadline expires, the binding signals OpenDAL's native cancel token and waits for the native call to drop its in-flight future before returning an `*opendal.Error` with `CodeCancelled` that unwraps to `context.Canceled` or `context.DeadlineExceeded`. Callers can inspect cancellations with `errors.Is(err, context.Canceled)`, `errors.Is(err, context.DeadlineExceeded)`, or `errors.As(err, &odErr)`.

For the full guide — connecting to real backends, streaming, listing, presigning, retries, and error handling — see the [user guide](https://opendal.apache.org/docs/bindings/go).

## Contributing

This document is for users of the binding. If you want to build the binding from source, run the tests, or run benchmarks, see [CONTRIBUTING.md](CONTRIBUTING.md). Contributions are welcome; please also read the project-wide [contributing guide](https://github.com/apache/opendal/blob/main/CONTRIBUTING.md).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
