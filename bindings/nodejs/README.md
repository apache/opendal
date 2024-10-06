# Apache OpenDAL™ Node.js Binding

![](https://img.shields.io/badge/status-released-blue)
[![npm](https://img.shields.io/npm/v/opendal.svg?logo=npm)](https://www.npmjs.com/package/opendal)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/nodejs/)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Installation

```shell
npm install opendal
```

## Docs

See our documentation on [opendal.apache.org](https://opendal.apache.org/docs/nodejs/).

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

## Contributing

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/opendal/issues/new) for bug report or feature requests.
- Asking questions in the [Discussions](https://github.com/apache/opendal/discussions/new?category=q-a).
  - Talk to community at [Discord](https://opendal.apache.org/discord).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
