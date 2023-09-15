# OpenDAL CPP Binding (WIP)

![](https://github.com/apache/incubator-opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Example

```cpp
#include "opendal.hpp"
#include <vector>

int main() {
    auto op = opendal::Operator("memory");
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    op.write("test", data);
    auto result = op.read("test");  // result == data
}
```

## Compiling

### Prerequisites

- CMake >= 3.10
- C++ compiler with C++17 support
- The boost library

### Build

```bash
mkdir build
cd build
# Add -DOPENDAL_DEV=ON to make development environment for OpenDAL
cmake ..
make
```

### Test

You should build the project with `OPENDAL_ENABLE_TESTING` option. Then run:

```bash
make test
```

### Docs

You should build the project with `OPENDAL_ENABLE_DOCUMENTATION` option. Then run:

```bash
make docs
```

### CMake Options

- `OPENDAL_DEV`: Enable development environment for OpenDAL. It will enable most development options. With this option, you don't need to set other options. Default: `OFF`
- `OPENDAL_ENABLE_ADDRESS_SANITIZER`: Enable address sanitizer. Default: `OFF`
- `OPENDAL_ENABLE_DOCUMENTATION`: Enable documentation. Default: `OFF`
- `OPENDAL_DOCS_ONLY`: Only build documentation. Default: `OFF`
- `OPENDAL_ENABLE_TESTING`: Enable testing. Default: `OFF`

## Using

### CMake

Because the project repo includes rust core code, we can't use `git submodule` directly to add it. So we recommend using `FetchContent` to add the library.

```cmake
FetchContent_Declare(
    opendal-cpp
    URL     https://github.com/apache/incubator-opendal/releases/download/<newest-tag>/opendal-cpp-<newest-tag>.tar.gz
    URL_HASH SHA256=<newest-tag-sha256>
)
FetchContent_MakeAvailable(opendal-cpp)
```