# OpenDAL Haskell Binding (WIP)

![](https://github.com/apache/incubator-opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Example

```haskell
import OpenDAL
import qualified Data.HashMap.Strict as HashMap

main :: IO ()
main = do
  Right op <- operator "memory" HashMap.empty
  runOp op operation
 where
  operation = do
    writeOp op "key1" "value1"
    writeOp op "key2" "value2"
    value1 <- readOp op "key1"
    value2 <- readOp op "key2"
```

## Build

1. Build OpenDAL Haskell Interface

```bash
cargo build --package opendal-hs
```

2. Build Haskell binding

If you don't want to install `libopendal_hs`, you need to specify library path manually by `LIBRARY_PATH=${OPENDAL_ROOT}/target/debug`.

```bash
LIBRARY_PATH=... cabal build
```

## Test

```bash
LD_LIBRARY_PATH=... cabal test
```

## Doc

```bash
cabal haddock
```