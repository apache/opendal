# OpenDAL Haskell Binding (WIP)

## Example

```haskell
import OpenDAL
import qualified Data.HashMap.Strict as HashMap

main :: IO ()
main = do
    Right op <- operator "memory" HashMap.empty
    _ <- write op "key1" "value1"
    _ <- write op "key2" "value2"
    value1 <- read op "key1"
    value2 <- read op "key2"
    value1 @?= "value1"
    value2 @?= "value2"
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