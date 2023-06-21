# OpenDAL LUA Binding (WIP)

## Example

```lua
local opendal = require("opendal")

local op, err = opendal.operator.new("fs",{root="/tmp"})
if err ~= nil then
    print(err)
    return
end
op:write("test.txt","hello world")
print("read: ", op:read("test.txt"))
```

## Build

1. Build OpenDAL LUA Interface

```bash
cargo build --package opendal-lua
```