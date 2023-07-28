# OpenDAL PHP Binding (WIP)

## Requirements

* PHP 8.1+

## Development

### Install Cargo PHP

we use [cargo-php](https://davidcole1340.github.io/ext-php-rs/getting-started/hello_world.html) to build the PHP extension.

```bash
cargo install cargo-php
```

### Build

Move to the `bindings/php` directory and run the following command, the extension will be built in `target/debug` directory and enable the extension in `php.ini` file.

```bash
cargo build
cargo php install
```

### Test

```bash
composer install
composer test
```

