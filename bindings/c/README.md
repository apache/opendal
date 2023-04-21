# OpenDAL C Binding (WIP)

# Build C bindings

To compile OpenDAL from source code, you'll need:
- C/C++ Toolchain: provides basic tools for compiling and linking OpenDAL C binding. This is available as `build-essential` on Ubuntu and similar names on other platforms.
- `clang-format`: OpenDAL uses clang-format to keep C binding code base clean. The `opendal.h` should be included and formatted by users manually.

```bash
make
make format
make build
make test
```

## License

[Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0)
