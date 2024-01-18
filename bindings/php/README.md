# Apache OpenDAL™ PHP Binding (WIP)

![](https://img.shields.io/badge/status-unreleased-red)

## Example

```php
use OpenDAL\Operator;

$op = new Operator("fs", ["root" => "/tmp"]);
$op->write("test.txt", "hello world");

echo $op->read("test.txt"); // hello world
```

## Requirements

* PHP 8.1+
* Composer

## Install Extension

We use [ext-php-rs](https://github.com/davidcole1340/ext-php-rs) to build PHP extensions natively in Rust, it's different from the traditional PHP extension development and cannot be installed using `pecl` or `phpize`. Before installing the extension, it is necessary to install Rust and Cargo. For instructions on how to install them, please refer to [Rust's website](https://www.rust-lang.org/tools/install).

1. Clone the repository

```bash
git clone git@github.com:apache/incubator-opendal.git
```

2. Build the opendal-php extension

```bash
cd incubator-opendal/bindings/php
cargo build
```

> don't forget to add `--release` flag for production use.

3. Enable extension for PHP Manually

```bash
cd incubator-opendal

# Linux
cp target/debug/libopendal_php.so $(php -r "echo ini_get('extension_dir');")/libopendal_php.so
echo "extension=libopendal_php.so" >> $(php -r "echo php_ini_loaded_file();")

# macOS
cp target/debug/libopendal_php.dylib $(php -r "echo ini_get('extension_dir');")/libopendal_php.dylib
echo "extension=libopendal_php.dylib" >> $(php -r "echo php_ini_loaded_file();")

# Windows
cp target/debug/libopendal_php.dll $(php -r "echo ini_get('extension_dir');")/libopendal_php.dll
echo "extension=libopendal_php.dll" >> $(php -r "echo php_ini_loaded_file();")
```

4. Enable extension for PHP using cargo-php

You can also use cargo-php directly to install the extension, see [cargo-php](https://davidcole1340.github.io/ext-php-rs/getting-started/cargo-php.html) for more details.

```bash
cargo install cargo-php
cd incubator-opendal/bindings/php
cargo php install
```
This command will automatically build the extension and copy it to the extension directory of the current PHP version.

5. Test

use `php -m` to check if the extension is installed successfully.

```bash
php -m | grep opendal-php
```

Composer test:

```bash
cd incubator-opendal/bindings/php

composer install
composer test
```

## Trademarks & Licenses

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
