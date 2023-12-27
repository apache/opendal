# Apache OpenDAL Node.js Binding

![](https://img.shields.io/badge/status-released-blue)
[![npm](https://img.shields.io/npm/v/opendal.svg?logo=npm)](https://www.npmjs.com/package/opendal)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/nodejs/)

![](https://github.com/apache/incubator-opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Installation

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

- Start with [Contributing Guide](https://github.com/apache/incubator-opendal/blob/main/bindings/nodejs/CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/incubator-opendal/issues/new) for bug report or feature requests.
- Asking questions in the [Discussions](https://github.com/apache/incubator-opendal/discussions/new?category=q-a).
- Talk to community at [Discord](https://discord.gg/XQy8yGR2dg).

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
