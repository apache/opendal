## Layer Features

- `layers-all`: Enable all layers support.
- `layers-metrics`: Enable metrics layer support.
- `layers-prometheus`: Enable prometheus layer support.
- `layers-tracing`: Enable tracing layer support.
- `layers-chaos`: Enable chaos layer support.

## Service Features

- `services-dashmap`: Enable dashmap service support.
- `services-ftp`: Enable ftp service support.
- `services-hdfs`: Enable hdfs service support.
- `services-memcached`: Enable memcached service support.
- `services-mini-moka`: Enable mini-moka service support.
- `services-moka`: Enable moka service support.
- `services-ipfs`: Enable ipfs service support.
- `services-redis`: Enable redis service support without TLS.
- `services-redis-rustls`: Enable redis service support with `rustls`.
- `services-redis-native-tls`: Enable redis service support with `native-tls`.
- `services-rocksdb`: Enable rocksdb service support.
- `services-atomicdata`: Enable atomicdata service support.
- `services-sled`: Enable sled service support.

## Dependencies Features

- `rustls`: Enable TLS functionality provided by `rustls`, enabled by default
- `native-tls`: Enable TLS functionality provided by `native-tls`
- `native-tls-vendored`: Enable the `vendored` feature of `native-tls`
