# Apache OpenDAL™ D Binding (WIP)

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/d)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

## Build

To compile OpenDAL d binding from source code, you need:

- [dmd/ldc/gdc](https://dlang.org/download)

```bash
# Test a specific backend
export OPENDAL_TEST=memory
# build libopendal_c (underneath call make -C ../c)
dub build -b release
# build and run unit tests
dub test
```

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
