# Apache OpenDAL™ Zig Binding (WIP)

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/zig)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Build

To compile OpenDAL from source code, you need:

- [Zig](https://ziglang.org/download) 0.14.0 or higher

```bash
# build libopendal_c (underneath call make -C ../c)
zig build libopendal_c
# build and run unit tests
zig build test --summary all
```

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
