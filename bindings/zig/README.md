# OpenDAL Zig Binding (WIP)

# Build Zig bindings

To compile OpenDAL from source code, you'll need:
- [Zig 0.11.0 or higher](https://ziglang.org/download), totally portable.

```bash
# build opendal_c library (call make -C ../c)
zig build opendal_c
# Build and run test
zig build test -fsummary
```

## License

[Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0)
