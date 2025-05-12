## RedisConfig

### `db`: `_int`

the number of DBs redis can take is unlimited

default is db 0

### `endpoint`: `str`

network address of the Redis service. Can be "tcp://127.0.0.1:6379", e.g.

default is "tcp://127.0.0.1:6379"

### `cluster_endpoints`: `str`

network address of the Redis cluster service. Can be "tcp://127.0.0.1:6379,tcp://127.0.0.1:6380,tcp://127.0.0.1:6381", e.g.

default is None

### `username`: `str`

the username to connect redis service.

default is None

### `password`: `str`

the password for authentication

default is None

### `root`: `str`

the working directory of the Redis service. Can be "/path/to/dir"

default is "/"

### `default_ttl`: `_duration`

The default ttl for put operations.

