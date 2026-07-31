# Apache OpenDAL™ .NET Binding

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/dotnet)

This package provides a native .NET binding for **Apache OpenDAL™**, a data access
layer that allows you to access various storage services in a unified way.

We release the OpenDAL .NET binding independently of the
[`opendal` crate](https://crates.io/crates/opendal) (Rust core). For updates
and compatibility, use the .NET binding version instead of the `opendal` crate
version.

## Useful Links

- **User guide**: [opendal.apache.org/docs/bindings/dotnet](https://opendal.apache.org/docs/bindings/dotnet) — install, connect, common tasks, and going to production.
- **Services & configuration**: [opendal.apache.org/services](https://opendal.apache.org/services)
- **OpenDAL Scheme reference**: [docs.rs/opendal — enum.Scheme](https://docs.rs/opendal/latest/opendal/enum.Scheme.html)

## Installation

This binding is not yet published to NuGet. Build it from source — you need the
[.NET SDK](https://dotnet.microsoft.com/en-us/download/dotnet) for `net8.0` or
`net10.0` and a Rust toolchain for the native library:

```bash
cargo build
dotnet build
```

## Quickstart

```csharp
using OpenDAL;
using System.Text;

// Configure a service, then build an operator from it.
using var op = new Operator("memory");

// The same verbs work on every service.
op.Write("hello.txt", Encoding.UTF8.GetBytes("Hello, World!"));
var bytes = op.Read("hello.txt");
Console.WriteLine(Encoding.UTF8.GetString(bytes));
Console.WriteLine(op.Stat("hello.txt").ContentLength);
```

To use a real backend, change the scheme and pass its configuration — the
operations stay identical:

```csharp
using var op = new Operator("s3", new Dictionary<string, string>
{
    ["bucket"] = "your_bucket",
    ["region"] = "your_region",
});
```

OpenDAL also has a first-class async API — every operation has an `…Async`
counterpart. See
[Getting started](https://opendal.apache.org/docs/bindings/dotnet/getting-started)
and [Connecting to your storage](https://opendal.apache.org/docs/bindings/dotnet/connecting)
for the full guide.

## Contributing

To build and test the binding from source:

```bash
cargo build
dotnet build
dotnet test
```

For a complete guide on building, testing, and contributing, see the project's
[CONTRIBUTING](https://github.com/apache/opendal/blob/main/CONTRIBUTING.md) guide.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
