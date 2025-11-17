# Apache OpenDAL™ Node.js Binding

[![](https://img.shields.io/badge/status-released-blue)](https://www.npmjs.com/package/opendal)
[![npm](https://img.shields.io/npm/v/opendal.svg?logo=npm)](https://www.npmjs.com/package/opendal)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/nodejs/)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

## Useful Links

- [Documentation](https://opendal.apache.org/docs/nodejs/)
- [Upgrade Guide](./upgrade.md)

## Installation

```shell
npm install opendal
```

## Docs

To build the docs locally, please run the following commands:

```shell
# Only need to run once unless you want to update the docs theme
pnpm run build:theme

# Build the docs
pnpm run docs
```

## Tests

Services behavior tests read necessary configs from env vars or the `.env` file.

You can copy [.env.example](/.env.example) to `$(pwd)/.env` and change the values on need, or directly set env vars with `export KEY=VALUE`.

Take `fs` for example, we need to enable bench on `fs` on `/tmp`:

```properties
OPENDAL_TEST=fs
OPENDAL_FS_ROOT=/tmp
```

You can run service behavior tests of enabled with the following command:

```shell
pnpm build && pnpm test
```

## Usage

```javascript
import { Operator } from "opendal";

async function main() {
  const op = new Operator("fs", { root: "/tmp" });
  await op.write("test", "Hello, World!");
  const bs = await op.read("test");
  console.log(new TextDecoder().decode(bs));
  const meta = await op.stat("test");
  console.log(`contentLength: ${meta.contentLength}`);
}

main();
```

## Layers

OpenDAL provides layers to add additional functionality to operators. You can chain multiple layers together to build powerful data access pipelines.

### Available Layers

- **RetryLayer** - Retry failed operations automatically
- **ConcurrentLimitLayer** - Limit concurrent requests
- **TimeoutLayer** - Add timeout for operations
- **LoggingLayer** - Log all operations
- **ThrottleLayer** - Limit bandwidth usage

### TimeoutLayer

Prevents operations from hanging indefinitely:

```javascript
import { Operator, TimeoutLayer } from "opendal";

const op = new Operator("fs", { root: "/tmp" });

const timeout = new TimeoutLayer();
timeout.timeout = 10000; // 10 seconds for non-IO operations (ms)
timeout.ioTimeout = 3000; // 3 seconds for IO operations (ms)
op.layer(timeout.build());
```

**Default values:** timeout: 60s, ioTimeout: 10s

### LoggingLayer

Add structured logging for debugging and monitoring:

```javascript
import { Operator, LoggingLayer } from "opendal";

const op = new Operator("fs", { root: "/tmp" });

const logging = new LoggingLayer();
op.layer(logging.build());

// All operations will be logged
await op.write("test.txt", "Hello World");
```

Enable logging output:

```bash
# Show debug logs
RUST_LOG=debug node app.js

# Show only OpenDAL logs
RUST_LOG=opendal::services=debug node app.js
```

### ThrottleLayer

Control bandwidth usage with rate limiting:

```javascript
import { Operator, ThrottleLayer } from "opendal";

const op = new Operator("s3", {
  bucket: "my-bucket",
  region: "us-east-1",
});

// Limit to 10 KiB/s with 10 MiB burst
const throttle = new ThrottleLayer(10 * 1024, 10 * 1024 * 1024);
op.layer(throttle.build());
```

**Parameters:**

- `bandwidth`: Maximum bytes per second
- `burst`: Maximum burst size (must be larger than any operation size)

### RetryLayer

Automatically retry temporary failed operations:

```javascript
import { Operator, RetryLayer } from "opendal";

const op = new Operator("s3", {
  bucket: "my-bucket",
  region: "us-east-1",
});

const retry = new RetryLayer();
retry.maxTimes = 3;
retry.jitter = true;
op.layer(retry.build());
```

### ConcurrentLimitLayer

Limit concurrent requests to storage services:

```javascript
import { Operator, ConcurrentLimitLayer } from "opendal";

const op = new Operator("s3", {
  bucket: "my-bucket",
  region: "us-east-1",
});

// Allow max 1024 concurrent operations
const limit = new ConcurrentLimitLayer(1024);
limit.httpPermits = 512; // Limit HTTP requests separately
op.layer(limit.build());
```

### Combining Multiple Layers

Stack multiple layers for comprehensive control:

```javascript
import {
  Operator,
  LoggingLayer,
  TimeoutLayer,
  RetryLayer,
  ThrottleLayer,
} from "opendal";

const op = new Operator("s3", {
  bucket: "my-bucket",
  region: "us-east-1",
});

// Layer 1: Logging for observability
const logging = new LoggingLayer();
op.layer(logging.build());

// Layer 2: Timeout protection
const timeout = new TimeoutLayer();
timeout.timeout = 30000;
timeout.ioTimeout = 10000;
op.layer(timeout.build());

// Layer 3: Retry on failures
const retry = new RetryLayer();
retry.maxTimes = 3;
retry.jitter = true;
op.layer(retry.build());

// Layer 4: Bandwidth throttling
const throttle = new ThrottleLayer(100 * 1024, 10 * 1024 * 1024);
op.layer(throttle.build());

// Now the operator has full production-ready protection
await op.write("data.json", JSON.stringify(data));
```

## Usage with Next.js

Config automatically be bundled by [Next.js](https://nextjs.org/docs/app/api-reference/config/next-config-js/serverExternalPackages).

```javascript
/** @type {import('next').NextConfig} */
const nextConfig = {
  serverExternalPackages: ["opendal"],
};

module.exports = nextConfig;
```

## Contributing

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/opendal/issues/new) for bug report or feature requests.
- Asking questions in the [Discussions](https://github.com/apache/opendal/discussions/new?category=q-a).
  - Talk to community at [Discord](https://opendal.apache.org/discord).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
