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

## Build

```bash
mkdir build

# Add -DCMAKE_EXPORT_COMPILE_COMMANDS=1 to generate compile_commands.json for clangd
cmake -DCMAKE_BUILD_TYPE=Debug -GNinja .. 

ninja
```

## Test

You should build the project first. Then run:

```bash
ninja test
```