## MemcachedConfig

### `endpoint`: `str`

network address of the memcached service.

For example: "tcp://localhost:11211"

### `root`: `str`

the working directory of the service. Can be "/path/to/dir"

default is "/"

### `username`: `str`

Memcached username, optional.

### `password`: `str`

Memcached password, optional.

### `default_ttl`: `_duration`

The default ttl for put operations.

