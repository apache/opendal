# Apache OpenDAL™ Go Binding (WIP)

![](https://img.shields.io/badge/status-unreleased-red)

opendal-go requires opendal-c to be installed.

```shell
cd bindings/c
make build
```

You will find `libopendal_c.so` under `{root}/target`.

Then, we need to add a `opendal_c.pc` files

```pc
libdir=/path/to/opendal/target/debug/
includedir=/path/to/opendal/bindings/c/include/

Name: opendal_c
Description: opendal c binding
Version:

Libs: -L${libdir} -lopendal_c
Cflags: -I${includedir}
```

And set the `PKG_CONFIG_PATH` environment variable to the directory where `opendal_c.pc` is located.

```shell
export PKG_CONFIG_PATH=/dir/of/opendal_c.pc
```

Then, we can build the go binding.

```shell
cd bindings/go
go build -tags dynamic .
```

To running the go binding tests, we need to tell the linker where to find the `libopendal_c.so` file.

```shell
expose LD_LIBRARY_PATH=/path/to/opendal/bindings/c/target/debug/
```

Then, we can run the tests.

```shell
go test -tags dynamic .
```

For benchmark

```shell
go test -bench=. -tags dynamic .
```

## Trademarks & Licenses

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
