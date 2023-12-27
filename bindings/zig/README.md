# Apache OpenDAL™ Zig Binding (WIP)

![](https://img.shields.io/badge/status-unreleased-red)

![](https://github.com/apache/incubator-opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Build

To compile OpenDAL from source code, you need:

- [Zig](https://ziglang.org/download) 0.11.0 or higher

> **Note**:
>
> 0.11.0 is not released yet. You can use master instead before the official 0.11.0 released.

```bash
# build libopendal_c (underneath call make -C ../c)
zig build libopendal_c
# build and run unit tests
zig build test --summary all
```

## License

[Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0)
