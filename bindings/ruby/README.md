# Apache OpenDAL™ Ruby Binding

[![Gem Version](https://img.shields.io/gem/v/opendal)](https://rubygems.org/gems/opendal)
[![Gem Downloads (for latest version)](https://img.shields.io/gem/dtv/opendal)](https://rubygems.org/gems/opendal)

OpenDAL's Ruby [gem](https://rubygems.org/gems/opendal): access S3, GCS, Azure Blob, the local filesystem, and 50+ more services through one API.

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

## Useful Links

- [User guide](https://opendal.apache.org/docs/bindings/ruby)
- [Services & configuration](https://opendal.apache.org/services)
- [API reference](https://opendal.apache.org/docs/ruby/)
- [Rust core documentation](https://docs.rs/opendal/latest/opendal/index.html)

## Installation

```shell
gem install opendal
```

Or add it to your `Gemfile`:

```ruby
# Gemfile
gem "opendal"
```

## Quickstart

```ruby
require "opendal"

op = OpenDal::Operator.new("memory", {})
op.write("hello.txt", "Hello, World!")
puts op.read("hello.txt") # => "Hello, World!"

op.list("").each do |entry|
  puts entry.path
end

op.delete("hello.txt")
```

Point it at a real backend by changing the scheme and passing its configuration:

```ruby
op = OpenDal::Operator.new("s3", {
  "bucket" => "my-bucket",
  "region" => "us-east-1",
})
```

See the [user guide](https://opendal.apache.org/docs/bindings/ruby) for connecting to storage, common tasks, middlewares, and error handling.

## Contributing

Development setup and contribution instructions live in [CONTRIBUTING.md](CONTRIBUTING.md).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
