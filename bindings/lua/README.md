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

## Lua version
You have to enable one of the features: lua54, lua53, lua52, lua51, luajit(52) or luau in `Cargo.toml`, according to the chosen Lua version. Default Lua version is 5.2.

## Build

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

# Modules
## operator
### operator
To load this module:
```lua
local operator = require "opendal.operator"
```
### operator.new(schema, options)
Create a new operator instance.
- `schema` - schema name, currently support all schemas in OpenDAL https://docs.rs/opendal/latest/opendal/enum.Scheme.html
- `options` - options for the operator, currently support map config for fs schema in OpenDAL https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.via_map
- return `operator`, `error` - operator instance and error if any error occurs

### operator:read(path)
operator must be created by `operator.new` before use this function.
- `path` - path to read
- return `string`, `error` - content of the file and error if any error occurs
equavalent to https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.read

### operator:write(path, content)
operator must be created by `operator.new` before use this function.
- `path` - path to write
- `content` - content to write
- return `error` if any error occurs
equavalent to https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.write

### operator:delete(path)
operator must be created by `operator.new` before use this function.
- `path` - path to delete
- return `error` if any error occurs
equavalent to https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.delete

### operator:is_exist(path)
operator must be created by `operator.new` before use this function.
- `path` - path to check
- return `error` if any error occurs
equavalent to https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.is_exist

### operator:stat(path)
operator must be created by `operator.new` before use this function.
- `path` - path to check
- return `opendal.metadata`, `error` metadata of the path and error if any error occurs
equavalent to https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.stat

## metadata
`metadata` instance must be returned by `operator:stat` function.
### metadata:is_dir()
- return `bool` if the path is a directory
equavalent to https://docs.rs/opendal/latest/opendal/struct.Metadata.html#method.is_dir

### metadata:is_file()
- return `bool` if the path is a file
equavalent to https://docs.rs/opendal/latest/opendal/struct.Metadata.html#method.is_file