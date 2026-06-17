---
title: Getting started
sidebar_label: Getting started
description: Run your first OpenDAL program in .NET, then point it at a real storage backend.
---

# Getting started

## Your first program

This program builds an operator, writes a file, reads it back, inspects its
metadata, and deletes it. It runs against the in-memory service with no
credentials, so you can run it as soon as the binding is built.

```csharp
using OpenDAL;
using System.Text;

// Configure a service, then build an operator from it.
using var op = new Operator("memory");

// The same verbs work on every service.
op.Write("hello.txt", Encoding.UTF8.GetBytes("Hello, World!"));

var bytes = op.Read("hello.txt");
Console.WriteLine(Encoding.UTF8.GetString(bytes));

var meta = op.Stat("hello.txt");
Console.WriteLine($"size = {meta.ContentLength} bytes");

op.Delete("hello.txt");
```

`Write` takes a `byte[]`, and `Read` returns a `byte[]`. The `using` declaration
disposes the operator and frees its native handle when it goes out of scope —
always dispose `Operator` instances.

## Point it at a real backend

Only the service changes; the operations stay identical. Pass the scheme and a
dictionary of configuration keys:

```csharp
using OpenDAL;
using System.Text;

using var op = new Operator("s3", new Dictionary<string, string>
{
    ["bucket"] = "my-bucket",
    ["region"] = "us-east-1",
});

op.Write("hello.txt", Encoding.UTF8.GetBytes("Hello from S3!"));
Console.WriteLine(Encoding.UTF8.GetString(op.Read("hello.txt")));
```

The binding bundles every service, so there is nothing extra to install. The
next page, [Connecting to your storage](./03-connecting.md), covers construction
and credentials in depth; [Services](/services) lists every backend and its keys.

## Async usage {#blocking-vs-async}

Every operation has an `…Async` counterpart that returns a `Task` and accepts a
`CancellationToken`. Await each call:

```csharp
using OpenDAL;
using System.Text;

using var op = new Operator("memory");

await op.WriteAsync("hello.txt", Encoding.UTF8.GetBytes("Hello, World!"));
var bytes = await op.ReadAsync("hello.txt");
Console.WriteLine(Encoding.UTF8.GetString(bytes));
```

## Executors {#executors}

Async operations run on a native Tokio runtime. By default OpenDAL uses a shared
executor, so you can call the `…Async` methods without configuring anything. To
control the worker-thread count, create an [`Executor`] and pass it to each call:

```csharp
using OpenDAL;
using System.Text;

using var executor = new Executor(2); // two Tokio worker threads
using var op = new Operator("memory");

await op.WriteAsync("hello.txt", Encoding.UTF8.GetBytes("Hello, World!"), executor);
var bytes = await op.ReadAsync("hello.txt", executor);
```

Keep an `Executor` alive for the full lifetime of the operations using it.
Disposing an `Executor` (or `Operator`) while awaited work is still running
throws `ObjectDisposedException`, so dispose them only after the awaited
operations complete. See [Going to production](./05-production.md#executor-and-lifetime)
for the lifetime rules.

[`Executor`]: https://github.com/apache/opendal/blob/main/bindings/dotnet/OpenDAL/Executor.cs
