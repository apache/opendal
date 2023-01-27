# Oay

Oay is OpenDAL Gateway that intends to convert different storage interfaces.

## Quick Start

Start a http gateway with fs as backend.

```shell
export OAY_ADDR=127.0.0.1:8080
export OAY_BACKEND_TYPE=fs
export OAY_BACKEND_FS_ROOT=/tmp/

./oay http
```
