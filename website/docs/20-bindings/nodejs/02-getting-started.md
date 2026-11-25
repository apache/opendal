---
title: Getting started
sidebar_label: Getting started
description: Run your first OpenDAL program in Node.js, then point it at a real storage backend.
---

# Getting started

## Your first program

This program builds an operator, writes a file, reads it back, inspects its
metadata, and deletes it. It runs against the in-memory service with no
credentials, so you can run it right after installing.

```shell
npm install opendal
```

```javascript file=bindings/nodejs/examples/getting-started.mjs region=quickstart
```

`write` accepts a `string` or a `Buffer`; `read` returns a `Buffer`. Decode it
with `new TextDecoder().decode(bs)` or `bs.toString()`.

## Point it at a real backend

Only the service changes; the operations stay identical. The scheme is the first
argument, and configuration is an options object:

```javascript
import { Operator } from "opendal";

const op = new Operator("s3", {
  bucket: "my-bucket",
  region: "us-east-1",
});

await op.write("hello.txt", "Hello from S3!");
console.log((await op.read("hello.txt")).toString());
```

[Connecting to your storage](./03-connecting.md) covers configuration and
credentials. [Services](/services) lists the backends and their configuration keys.

## Build with additional services

The npm native packages enable `services-all` on Linux and macOS and
`default` on Windows. Both groups are listed in the binding's
[Cargo.toml](https://github.com/apache/opendal/blob/main/bindings/nodejs/Cargo.toml).

To add FTP to the Linux or macOS release service set, run these commands
from `bindings/nodejs` in a POSIX shell:

```shell
pnpm install --frozen-lockfile
NAPI_FEATURES=services-all,services-ftp pnpm build
```

On Windows, set `$env:NAPI_FEATURES = "services-ftp"` in PowerShell before
running `pnpm build` to add FTP to the default services.

The [Node.js layers](./05-production.md#layers) are built in. Exposing a new
Rust layer requires Node.js binding code.

## Async and sync {#blocking-vs-async}

OpenDAL's Node.js API is promise-based: every operation returns a `Promise` you
`await`. For synchronous code, each verb has a `*Sync` variant that blocks and
returns the value directly:

```javascript
import { Operator } from "opendal";

const op = new Operator("memory");

op.writeSync("hello.txt", "Hello, World!");
const bs = op.readSync("hello.txt");
console.log(bs.toString());
```

The sync variants cover the same operations (`statSync`, `listSync`,
`deleteSync`, …). Use the async API in servers and request handlers; reach for
the sync variants only in scripts and startup code where blocking is acceptable.
</content>
