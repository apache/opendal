# OpenDAL Haskell Binding (WIP)

## Example

```haskell
import OpenDAL
import qualified Data.HashMap.Strict as HashMap

main :: IO ()
main = do
  op <- createOp "memory" HashMap.empty
    case op of
      Left e -> assertFailure $ "Failed to create operator, " ++ e
      Right op' -> do
        _ <- writeOp op' "key1" "value1"
        _ <- writeOp op' "key2" "value2"
        value1 <- readOp op' "key1"
        value2 <- readOp op' "key2"
        value1 @?= Right "value1"
        value2 @?= Right "value2"
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