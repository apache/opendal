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

Install a packaged gem for the active Ruby ABI into an isolated gem directory,
then smoke-test it with RubyGems 4.1 or later. Replace the example filename with
the conventional platform gem or the content-addressable gem for that ABI:

```shell
gem_file=pkg/opendal-VERSION-PLATFORM_OR_CONTENT_ADDRESS.gem
export GEM_HOME="$(ruby -rtmpdir -e 'puts Dir.mktmpdir("opendal-gem-smoke-")')"
export GEM_PATH="$GEM_HOME"
unset RUBYOPT RUBYLIB BUNDLE_GEMFILE
gem install --local "$gem_file" --no-document
ruby test/package/smoke.rb "$gem_file"
```
