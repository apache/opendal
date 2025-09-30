# Apache OpenDAL™ Ruby Binding

![Gem Version](https://img.shields.io/gem/v/opendal)
![Gem Downloads (for latest version)](https://img.shields.io/gem/dtv/opendal)

OpenDAL's Ruby gem.

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Get started

### Installation

Install gem:

```shell
bundle add opendal
```

or add it in Gemfile:

```ruby
# Gemfile

source "https://rubygems.org"

gem 'opendal'
```

### Examples

#### File operations with an in-memory storage 

```ruby
require 'opendal'

op = OpenDal::Operator.new("memory", {})
op.write("file", "hello world")
puts op.read("file") # => "hello world"
puts ""

puts "List:", op.list("").map { |e| e.path }
puts ""

puts "Stat"
puts op.stat("file").inspect # => #<OpenDal::Metadata mode: File,         content_type: ,         content_length: 11>
puts ""

puts "Deleting 'file'"
op.delete("/file")
puts ""

puts "Exist?", op.exist?("/file") # => false
puts ""

puts "Info:", op.info.inspect # => #<OpenDal::OperatorInfo scheme: "memory", root: "/">
```

#### A S3 operator

```ruby
require 'opendal'

op = OpenDal::Operator.new("s3", {
  "endpoint" => "http://localhost:9000",
  "access_key_id" => "minioadmin" ,
  "secret_access_key" => "minioadmin",
  "bucket" => "test",
  "region" => "us-east-1",
})
op.write("file", "hello world")
puts op.read("file") # => "hello world"
puts ""

puts "List:", op.list("").map { |e| e.path }
puts ""

puts "Stat"
puts op.stat("file").inspect # => #<OpenDal::Metadata mode: File,         content_type: binary/octet-stream,         content_length: 11>
puts ""

puts "Deleting 'file'"
op.delete("file")
puts ""

puts "Exist?", op.exist?("file") # => false
puts ""

puts "Info:", op.info.inspect # => #<OpenDal::OperatorInfo scheme: "s3", root: "/">
```

#### Use middleware

```ruby
require 'opendal'

op = OpenDal::Operator.new("s3", {
  "endpoint" => "http://localhost:9000",
  "access_key_id" => "minioadmin" ,
  "secret_access_key" => "minioadmin",
  "bucket" => "test",
  "region" => "us-east-1",
})

op.middleware(OpenDal::Middleware::ConcurrentLimit.new(5))
op.middleware(OpenDal::Middleware::Retry.new)
op.middleware(OpenDal::Middleware::Timeout.new(1, 2))

op.list("/").map do |e|
  puts e.inspect
end
```

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
bundle exec rake test
```

Run linters:

```shell
bundle exec rake standard:fix
rustfmt --config-path ../../rustfmt.toml src/*.rs # Run rustfmt for Rust files
cargo clippy --fix --all-targets # Run rust linter clippy
```

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
