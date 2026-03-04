# Apache OpenDAL™ .Net Binding (WIP)

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/dotnet)

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

This binding is currently under development. Please check back later.

## Build

To compile OpenDAL .NET binding from source code, you need:

- [.NET](https://dotnet.microsoft.com/en-us/download/dotnet) version 10.0

```bash
cargo build
dotnet build
dotnet test
```

## Usage

```csharp
using DotOpenDAL;

var op = new Operator("memory");
var fs = new Operator("fs", new Dictionary<string, string>
{
	["root"] = "/tmp/opendal",
});

using var executor = new Executor(2);
op.Write("path", System.Text.Encoding.UTF8.GetBytes("hello"), executor);
var content = await op.ReadAsync("path", executor);
```

## Recommended Usage

- Prefer `using` for both `Operator` and `Executor` to release native handles deterministically.
- Keep `Executor` alive for the entire lifetime of operations that use it.
- Do not dispose `Executor` before pending async operations complete.

```csharp
using DotOpenDAL;
using System.Text;

using var executor = new Executor(2);
using var op = new Operator("memory");

await op.WriteAsync("demo", Encoding.UTF8.GetBytes("hello"), executor);
var data = await op.ReadAsync("demo", executor);
```

If you don't pass an `Executor`, OpenDAL uses the default executor.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
