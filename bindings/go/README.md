# OpenDAL Golang Binding

Build C binding first.

cd `bindings/c`

```shell
cargo build
```

and than cd `bindings/go`

```shell
CGO_ENABLED=0 LD_LIBRARY_PATH=../../target/debug go test -v
```

We can call it without CGO (only work under linux).
