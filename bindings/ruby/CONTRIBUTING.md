# Contributing to Apache OpenDAL™ Ruby Binding

## Development

Install gem and its dependencies:

```shell
bundle
```

Build bindings:

```shell
bundle exec rake compile
```

Run tests:

```shell
bundle exec rake test:base

# Optional
OPENDAL_TEST=fs bundle exec rake test:service
```

Run linters:

```shell
bundle exec rake standard:fix
rustfmt --config-path ../../rustfmt.toml src/*.rs # Run rustfmt for Rust files
cargo clippy --fix --all-targets # Run rust linter clippy
```

## Native gems

Compile each supported Ruby ABI before packaging:

```shell
bundle exec rake native:compile # Run with Ruby 3.3, 3.4, and 4.0.
```

Use RubyGems 4.1 or later to package the compiled extensions and check the
artifacts:

```shell
bundle exec rake native:opendal test:package
```

This produces one conventional platform gem and one content-addressable gem
for each Ruby ABI in `pkg/`. The source gem still uses `bundle exec rake build`.

Install and smoke-test the gem for the active Ruby ABI with RubyGems 4.1 or later:

```shell
OPENDAL_RUBY_GEM_FORMAT=content-addressable ruby test/package/smoke.rb
```

Use `OPENDAL_RUBY_GEM_FORMAT=conventional` to check the multi-ABI gem instead.
