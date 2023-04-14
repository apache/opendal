# OpenDAL Node.js Binding

## Installation

### Node.js

```shell
npm install opendal
```

## Docs

See our documentations on [opendal.apache.org](https://opendal.apache.org/docs/nodejs/).

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

- Install latest `Rust`
- Install `Node.js@10+` which fully supported `Node-API`

We are using `corepack` to specific package manager:

```shell
corepack enable
```

`corepack` is distributed with Node.js, so you do not need to specifically look for a way to install it.

### Build

```bash
# Install dependencies.
yarn
# Build from source.
yarn build
# Build from source with debug info.
yarn build:debug
```

### Test

```bash
yarn test
```

We use [`Cucumber`](https://cucumber.io/) for behavior testing. Refer to [here](https://cucumber.io/docs/guides/overview/) for more information about `Cucumber`.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
