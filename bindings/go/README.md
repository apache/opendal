# Apache OpenDAL™ Go Binding

[![](https://img.shields.io/badge/status-released-blue)](https://pkg.go.dev/github.com/apache/opendal/bindings/go)
[![Go Reference](https://pkg.go.dev/badge/github.com/apache/opendal/bindings/go.svg)](https://pkg.go.dev/github.com/apache/opendal/bindings/go)

opendal-go is a **Native** support Go binding without CGO enabled and is built on top of opendal-c.

```bash
go get github.com/apache/opendal/bindings/go@latest
```

opendal-go requires **libffi** to be installed.

## Basic Usage

```go
package main

import (
	"fmt"
	"github.com/apache/opendal-go-services/memory"
	opendal "github.com/apache/opendal/bindings/go"
)

func main() {
	// Initialize a new in-memory operator
	op, err := opendal.NewOperator(memory.Scheme, opendal.OperatorOptions{})
	if err != nil {
		panic(err)
	}
	defer op.Close()

	// Write data to a file named "test"
	err = op.Write("test", []byte("Hello opendal go binding!"))
	if err != nil {
		panic(err)
	}

	// Read data from the file "test"
	data, err := op.Read("test")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read content: %s\n", data)

	// List all entries under the root directory "/"
	lister, err := op.List("/")
	if err != nil {
		panic(err)
	}
	defer lister.Close()

	// Iterate through all entries
	for lister.Next() {
		entry := lister.Entry()

		// Get entry name (not used in this example)
		_ = entry.Name()

		// Get metadata for the current entry
		meta, _ := op.Stat(entry.Path())

		// Print file size
		fmt.Printf("Size: %d bytes\n", meta.ContentLength())

		// Print last modified time
		fmt.Printf("Last modified: %s\n", meta.LastModified())

		// Check if the entry is a directory or a file
		fmt.Printf("Is directory: %v, Is file: %v\n", meta.IsDir(), meta.IsFile())
	}

	// Check for any errors that occurred during iteration
	if err := lister.Error()\;\; err != nil {
		panic(err)
	}

	// Copy a file
	op.Copy("test", "test_copy")

	// Rename a file
	op.Rename("test", "test_rename")

	// Delete a file
	op.Delete("test_rename")
}
```

## Run Tests

### Behavior Tests

```bash
cd tests/behavior_tests
# Test a specific backend
export OPENDAL_TEST=memory
# Run all tests
CGO_ENABLE=0 go test -v -run TestBehavior
# Run specific test
CGO_ENABLE=0 go test -v -run TestBehavior/Write
# Run synchronously
CGO_ENABLE=0 GOMAXPROCS=1 go test -v -run TestBehavior
```

### Benchmark

```bash
cd tests/behavior_tests
# Benchmark a specific backend
export OPENDAL_TEST=memory

go test -bench .
```

<details>
  <summary>
  A benchmark between [purego+libffi](https://github.com/apache/opendal/commit/bf15cecd5e3be6ecaa7056b5594589c9f4d85673) vs [CGO](https://github.com/apache/opendal/commit/9ef494d6df2e9a13c4e5b9b03bcb36ec30c0a7c0)
  </summary>

**purego+libffi** (as `new.txt`)
```
goos: linux
goarch: arm64
pkg: github.com/apache/opendal/bindings/go
BenchmarkWrite4KiB-10            1000000              2844 ns/op
BenchmarkWrite256KiB-10           163346             10092 ns/op
BenchmarkWrite4MiB-10              12900             99161 ns/op
BenchmarkWrite16MiB-10              1785            658210 ns/op
BenchmarkRead4KiB-10              194529              6387 ns/op
BenchmarkRead256KiB-10             14228             82704 ns/op
BenchmarkRead4MiB-10                 981           1227872 ns/op
BenchmarkRead16MiB-10                328           3617185 ns/op
PASS
ok
```

**CGO** (as `old.txt`)
```
go test -bench=. -tags dynamic .
goos: linux
goarch: arm64
pkg: opendal.apache.org/go
BenchmarkWrite4KiB-10             241981              4240 ns/op
BenchmarkWrite256KiB-10           126464             10105 ns/op
BenchmarkWrite4MiB-10              13443             89578 ns/op
BenchmarkWrite16MiB-10              1737            646155 ns/op
BenchmarkRead4KiB-10               53535             20939 ns/op
BenchmarkRead256KiB-10              9008            132738 ns/op
BenchmarkRead4MiB-10                 576           1846683 ns/op
BenchmarkRead16MiB-10                230           6305322 ns/op
PASS
ok
```

**Diff** with [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
```
benchstat old.txt new.txt
goos: linux
goarch: arm64
pkg: github.com/apache/opendal/bindings/go
               │   new.txt    │
               │    sec/op    │
Write4KiB-10     2.844µ ± ∞ ¹
Write256KiB-10   10.09µ ± ∞ ¹
Write4MiB-10     99.16µ ± ∞ ¹
Write16MiB-10    658.2µ ± ∞ ¹
Read4KiB-10      6.387µ ± ∞ ¹
Read256KiB-10    82.70µ ± ∞ ¹
Read4MiB-10      1.228m ± ∞ ¹
Read16MiB-10     3.617m ± ∞ ¹
geomean          90.23µ
¹ need >= 6 samples for confidence interval at level 0.95

pkg: opendal.apache.org/go
               │   old.txt    │
               │    sec/op    │
Write4KiB-10     4.240µ ± ∞ ¹
Write256KiB-10   10.11µ ± ∞ ¹
Write4MiB-10     89.58µ ± ∞ ¹
Write16MiB-10    646.2µ ± ∞ ¹
Read4KiB-10      20.94µ ± ∞ ¹
Read256KiB-10    132.7µ ± ∞ ¹
Read4MiB-10      1.847m ± ∞ ¹
Read16MiB-10     6.305m ± ∞ ¹
geomean          129.7µ
¹ need >= 6 samples for confidence interval at level 0.95
```
</details>

## Capabilities

- [x] OperatorInfo
- [x] Stat
    - [x] Metadata
- [x] IsExist
- [x] Read
    - [x] Read
    - [x] Reader -- implement as `io.ReadCloser`
- [ ] Write
    - [x] Write
    - [ ] Writer -- Need support from the C binding
- [x] Delete
- [x] CreateDir
- [ ] Lister
    - [x] Entry
    - [ ] Metadata -- Need support from the C binding
- [x] Copy
- [x] Rename

## Development

The guide is based on Linux. For other platforms, please adjust the commands accordingly.

To develop the Go binding, you need to have the following dependencies installed:

- zstd
- Rust toolchain
- Go

We use `go workspace` to manage and build the dependencies. To set up the workspace, run the following commands:

**Linux and macOS**

```bash
mkdir opendal_workspace
cd opendal_workspace
git clone --depth 1 git@github.com:apache/opendal.git
git clone --depth 1 git@github.com:apache/opendal-go-services.git

go work init
go work use ./opendal/bindings/go
go work use ./opendal/bindings/go/tests/behavior_tests
# use the backend you want to test, e.g., fs or memory
go work use ./opendal-go-services/fs
go work use ./opendal-go-services/memory

cat <<EOF > ./make_test.sh
#!/bin/bash

architecture=\$(uname -m)
if [ "\$architecture" = "x86_64" ]\;\; then
    ARCH="x86_64"
elif [ "\$architecture" = "aarch64" ] || [ "\$architecture" = "arm64" ]\;\; then
    ARCH="arm64"
else
    ARCH="unknown"
fi

cd opendal/bindings/c
cargo build
cd -
zstd -19 opendal/bindings/c/target/debug/libopendal_c.so -o opendal-go-services/fs/libopendal_c.linux.\$ARCH.so.zst

go test ./opendal/bindings/go/tests/behavior_tests -v -run TestBehavior
EOF

chmod +x ./make_test.sh

cd -
```

To build and run tests, run the following commands:

```bash
cd opendal_workspace

# specify the backend to test
export OPENDAL_TEST=fs
export OPENDAL_FS_ROOT=/tmp/opendal

# build the C binding and run the tests
./make_test.sh

cd -
```

<details>
  <summary>
  For Windows
  </summary>

```powershell
New-Item -ItemType Directory -Path opendal_workspace
Set-Location -Path opendal_workspace

git clone --depth 1 git@github.com:apache/opendal.git
git clone --depth 1 git@github.com:apache/opendal-go-services.git

go work init
go work use ./opendal/bindings/go
go work use ./opendal/bindings/go/tests/behavior_tests
# use the backend you want to test, e.g., fs or memory
go work use ./opendal-go-services/fs
go work use ./opendal-go-services/memory

@'
# Specify the backend to test\;Set-Item -Path Env:SERVICE -Value "$env:OPENDAL_TEST"\;# Get architecture\;$architecture = (Get-WmiObject Win32_OperatingSystem).OSArchitecture\;\;if ($architecture -like "*64*") {\;    $ARCH = "x86_64"\;} else {\;    $ARCH = "unknown" \;}\;\;# Build opendal\;Push-Location opendal/bindings/c\;cargo build\;Pop-Location\;\;# Rename dll file\;Rename-Item opendal/bindings/c/target/debug/opendal_c.dll libopendal_c.dll\;\;# Set environment variables\;Set-Item -Path Env:GITHUB_WORKSPACE -Value "$PWD/opendal-go-services"\;Set-Item -Path Env:VERSION -Value "latest"\;Set-Item -Path Env:TARGET -Value "windows"\;Set-Item -Path Env:DIR -Value "$($env:GITHUB_WORKSPACE)/libopendal_c_$($env:VERSION)_$($env:SERVICE)_$($env:TARGET)"\;\;if (-not (Test-Path $env:DIR)) {\;    New-Item -ItemType Directory -Path $env:DIR\;}\;\;# Compress with zstd\;zstd -19 opendal/bindings/c/target/debug/libopendal_c.dll -o "$($env:DIR)/libopendal_c.windows.dll.zst"\;\;Push-Location opendal-go-services/internal/generate\;go run generate.go\;Pop-Location\;\;# Set environment variables\;Set-Item -Path Env:MATRIX -Value '{"build": [{"target":"windows", "goos":"windows", "goarch": "amd64"}], "service": ["fs"]}'\;# Assume that libffi-8.dll is in the root of workspace directory\;Set-Item -Path Env:PATH -Value "$($env:PATH);$PWD"\;# Run tests\;go test ./opendal/bindings/go/tests/behavior_tests -v -run TestBehavior\;
'@ -replace "\\;","`n" | Out-File -FilePath "MakeTest.ps1" -Encoding UTF8

Pop-Location
```

To build and run tests, run the following commands:

```powershell
Set-Location -Path opendal_workspace
# specify the backend to test
$env:OPENDAL_TEST = "fs"
$env:OPENDAL_FS_ROOT = $env:TEMP

# build the C binding and run the tests
.\MakeTest.ps1

Pop-Location
```
</details>

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
