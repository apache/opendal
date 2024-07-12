# Apache OpenDAL™ Go Binding (WIP)

![](https://img.shields.io/badge/status-unreleased-red)

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
	"os"

	"github.com/yuchanns/opendal-go-services/memory"
	"github.com/apache/opendal/bindings/go"
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
	if err := lister.Error(); err != nil {
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

```bash
# Run all tests
CGO_ENABLE=0 go test -v -run TestBehavior
# Run specific test
CGO_ENABLE=0 go test -v -run TestBehavior/Write
# Run synchronously
CGO_ENABLE=0 GOMAXPROCS=1 go test -v -run TestBehavior
```

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


## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
