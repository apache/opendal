# Apache OpenDAL™ .NET Binding

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/dotnet)

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

## Build

To compile OpenDAL .NET binding from source code, you need:

- [.NET SDK](https://dotnet.microsoft.com/en-us/download/dotnet) for `net8.0` or `net10.0`.
- Rust toolchain for building the native library.

```bash
cargo build
dotnet build
dotnet test
```

## Quickstart

```csharp
using OpenDAL;
using System.Text;

using var executor = new Executor(2);
using var op = new Operator("memory");

await op.WriteAsync("demo.txt", Encoding.UTF8.GetBytes("hello"), executor);

var bytes = await op.ReadAsync("demo.txt", executor);
var text = Encoding.UTF8.GetString(bytes);

var meta = await op.StatAsync("demo.txt", executor: executor);
var entries = await op.ListAsync("", executor: executor);

await op.DeleteAsync("demo.txt", executor);
```

If you don't pass an `Executor`, OpenDAL uses the default executor.

## Creating an Operator

### With scheme + dictionary options

```csharp
using OpenDAL;

using var fs = new Operator("fs", new Dictionary<string, string>
{
	["root"] = "/tmp/opendal",
});
```

### With typed service config

```csharp
using OpenDAL;
using OpenDAL.ServiceConfig;

using var fs = new Operator(new FsServiceConfig
{
	Root = "/tmp/opendal",
});
```

## Executor and Lifetime

- Prefer `using` for `Operator`, `Executor`, and stream instances.
- Keep an `Executor` alive for the full lifetime of operations using it.
- Disposing an `Executor` or `Operator` too early causes `ObjectDisposedException`.
- For async calls, ensure disposal happens after awaited operations complete.

## Core Operations

`Operator` provides sync and async APIs for common object operations:

- Read / Write
- Stat / List
- Delete / CreateDir
- Copy / Rename / RemoveAll
- PresignRead / PresignWrite / PresignStat / PresignDelete (async)
- `Info` for scheme/root/name/capabilities
- `Duplicate()` to create a new handle to the same backend configuration

```csharp
using OpenDAL;
using OpenDAL.Options;
using System.Text;

using var op = new Operator("memory");

op.CreateDir("logs/");
op.Write("logs/a.txt", Encoding.UTF8.GetBytes("v1"));
op.Copy("logs/a.txt", "logs/b.txt");
op.Rename("logs/b.txt", "logs/c.txt");

var read = op.Read("logs/c.txt", new ReadOptions { Offset = 0, Length = 2 });
var list = op.List("logs/", new ListOptions { Recursive = true });

op.RemoveAll("logs/");
```

## Options

Use options types in `OpenDAL.Options` to pass operation-specific parameters.

- `ReadOptions`: range, conditions, concurrency, response-header overrides.
- `WriteOptions`: append, content headers, conditions, concurrency/chunk, user metadata.
- `StatOptions`: conditional headers and response-header overrides.
- `ListOptions`: recursive, limit, start-after, versions, deleted.

```csharp
using OpenDAL.Options;

var writeOptions = new WriteOptions
{
	ContentType = "text/plain",
	CacheControl = "no-cache",
	IfNotExists = true,
};

await op.WriteAsync("docs/readme.txt", content, writeOptions);

var listOptions = new ListOptions
{
	Recursive = true,
	Limit = 100,
};

var entries = await op.ListAsync("docs/", listOptions);
```

## Streams

OpenDAL exposes .NET `Stream` wrappers:

- `OpenReadStream(path, readOptions, executor)` returns `OperatorInputStream`.
- `OpenWriteStream(path, writeOptions, bufferSize, executor)` returns `OperatorOutputStream`.

```csharp
using OpenDAL;
using System.Text;

using var op = new Operator("memory");

await using (var writer = op.OpenWriteStream("stream.txt"))
{
	var payload = Encoding.UTF8.GetBytes("stream-data");
	await writer.WriteAsync(payload, 0, payload.Length);
	await writer.FlushAsync();
}

using var reader = op.OpenReadStream("stream.txt");
using var ms = new MemoryStream();
reader.CopyTo(ms);
```

Notes:

- Dispose write streams to flush/close native resources deterministically.
- `ReadAsync` / `WriteAsync` on these streams are synchronous wrappers that still honor cancellation checks before execution.

## Presign

Generate presigned HTTP requests with expiration:

```csharp
using OpenDAL;

using var op = new Operator("s3", new Dictionary<string, string>
{
	["bucket"] = "my-bucket",
	["region"] = "us-east-1",
	["access_key_id"] = "<ak>",
	["secret_access_key"] = "<sk>",
});

var request = await op.PresignReadAsync("data/file.txt", TimeSpan.FromMinutes(10));

Console.WriteLine(request.Method);
Console.WriteLine(request.Uri);
foreach (var header in request.Headers)
{
	Console.WriteLine($"{header.Key}: {header.Value}");
}
```

Available APIs:

- `PresignReadAsync`
- `PresignWriteAsync`
- `PresignStatAsync`
- `PresignDeleteAsync`

## Layers

Apply middleware-like behavior with `WithLayer`:

```csharp
using OpenDAL;
using OpenDAL.Layer;

using var baseOp = new Operator("memory");

using var op = baseOp
	.WithLayer(new ConcurrentLimitLayer(64))
	.WithLayer(new RetryLayer
	{
		MaxTimes = 5,
		MinDelay = TimeSpan.FromMilliseconds(100),
		MaxDelay = TimeSpan.FromSeconds(5),
		Factor = 2f,
		Jitter = true,
	})
	.WithLayer(new TimeoutLayer
	{
		Timeout = TimeSpan.FromSeconds(30),
		IoTimeout = TimeSpan.FromSeconds(5),
	});
```

## Error Handling and Capability Checks

Most failures are surfaced as `OpenDALException` with a typed `Code` (`ErrorCode`).

```csharp
try
{
	await op.ReadAsync("missing.txt");
}
catch (OpenDALException ex) when (ex.Code == ErrorCode.NotFound)
{
	// Handle missing object
}
```

Some features depend on backend capability. Check `op.Info.FullCapability` before using optional options/features:

```csharp
var cap = op.Info.FullCapability;
if (cap.PresignRead)
{
	var req = await op.PresignReadAsync("a.txt", TimeSpan.FromMinutes(5));
}
```

## Path Conventions

- Use backend-native object keys (for example, `a/b/c.txt`).
- For directory-like operations (`CreateDir`, directory `Stat`, `List` roots), prefer trailing slash paths such as `logs/`.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
