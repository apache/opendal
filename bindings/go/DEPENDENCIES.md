# Dependencies

OpenDAL Go Binding is based on the C Binding (`opendal-c`) and invokes it directly via `purego` without CGO or `libffi`.
Companion `opendal-go-services` packages must embed an `opendal-c` shared library built with the matching C ABI.
