# Apache OpenDAL™ Lua Binding (WIP)

![](https://img.shields.io/badge/status-unreleased-red)

![](https://github.com/apache/incubator-opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

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

## Lua version
You have to enable one of the features: lua54, lua53, lua52, lua51, luajit(52) or luau in `Cargo.toml`, according to the chosen Lua version. Default Lua version is 5.2.

## Build from source

1. Build OpenDAL LUA Interface

```bash
$ cd bindings/lua
$ cargo build --package opendal-lua --release
```

2. Install opendal lua library
```bash
# copy to lua share library directory
# default lua5.2 share library directory is /usr/lib/lua/5.2
$ cp ../../target/release/libopendal_lua.so /usr/lib/lua/5.2/opendal.so
```

## Install from luarocks
```bash
$ luarocks make
```

## Usage
```bash
$ lua5.2 example/fs.lua
read:   hello world
```

## Test
```bash
$ busted -o gtest test/opendal_test.lua
[==========] Running tests from scanned files.
[----------] Global test environment setup.
[----------] Running tests from test/opendal_test.lua
[ RUN      ] test/opendal_test.lua @ 24: opendal unit test opendal fs schema operator function in fs schema
[       OK ] test/opendal_test.lua @ 24: opendal unit test opendal fs schema operator function in fs schema (1.52 ms)
[ RUN      ] test/opendal_test.lua @ 36: opendal unit test opendal fs schema meta function in fs schema
[       OK ] test/opendal_test.lua @ 36: opendal unit test opendal fs schema meta function in fs schema (0.24 ms)
[----------] 2 tests from test/opendal_test.lua (3.47 ms total)

[----------] Global test environment teardown.
[==========] 2 tests from 1 test file ran. (3.54 ms total)
[  PASSED  ] 2 tests.
```

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
