/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package opendal

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

// Check verifies if the operator is functioning correctly.
//
// This function performs a health check on the operator by sending a `list` request
// to the root path. It returns any errors encountered during this process.
//
// # Returns
//
//   - error: An error if the check fails, or nil if the operator is working correctly.
//
// # Details
//
// The check is performed by attempting to list the contents of the root directory.
// This operation tests the basic functionality of the operator, including
// connectivity and permissions.
//
// # Example
//
//	func exampleCheck(op *opendal.Operator) {
//		err = op.Check()
//		if err != nil {
//			log.Printf("Operator check failed: %v", err)
//		} else {
//			log.Println("Operator is functioning correctly")
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Check() (err error) {
	ds, err := op.List("/")
	if err != nil {
		return
	}
	defer ds.Close()
	ds.Next()
	err = ds.Error()
	if err, ok := err.(*Error); ok && err.Code() == CodeNotFound {
		return nil
	}
	return
}

// List returns a Lister to iterate over entries that start with the given path in the parent directory.
//
// This function creates a new Lister to enumerate entries in the specified path.
//
// # Parameters
//
//   - path: The starting path for listing entries.
//
// # Returns
//
//   - *Lister: A new Lister instance for iterating over entries.
//   - error: An error if the listing operation fails, or nil if successful.
//
// # Notes
//
//  1. List is a wrapper around the C-binding function `opendal_operator_list`. Recursive listing is not currently supported.
//  2. Returned entries do not include metadata information. Use op.Stat to fetch metadata for individual entries.
//
// # Example
//
//	func exampleList(op *opendal.Operator) {
//		lister, err := op.List("test")
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		for lister.Next() {
//			entry := lister.Entry()
//
//			meta, err := op.Stat(entry.Path())
//			if err != nil {
//				log.Printf("Error fetching metadata for %s: %v", entry.Path(), err)
//				continue
//			}
//
//			fmt.Printf("Name: %s\n", entry.Name())
//			fmt.Printf("Length: %d\n", meta.ContentLength())
//			fmt.Printf("Last Modified: %s\n", meta.LastModified())
//			fmt.Printf("Is Directory: %v, Is File: %v\n", meta.IsDir(), meta.IsFile())
//			fmt.Println("---")
//		}
//		if err := lister.Err(); err != nil {
//			log.Printf("Error during listing: %v", err)
//		}
//	}
//
// Note: Always check lister.Err() after the loop to catch any errors that
// occurred during iteration.
func (op *Operator) List(path string) (*Lister, error) {
	list := getFFI[operatorList](op.ctx, symOperatorList)
	inner, err := list(op.inner, path)
	if err != nil {
		return nil, err
	}
	lister := &Lister{
		inner: inner,
		ctx:   op.ctx,
	}
	return lister, nil
}

// Lister provides an mechanism for listing entries at a specified path.
//
// Lister is a wrapper around the C-binding function `opendal_operator_list`. It allows
// for efficient iteration over entries in a storage system.
//
// # Limitations
//
//   - The current implementation does not support the `list_with` functionality.
//
// # Usage
//
// Lister should be used in conjunction with its Next() and Entry() methods to
// iterate through entries. The iteration ends when there are no more entries
// or when an error occurs.
//
// # Behavior
//
//   - Next() returns false when there are no more entries or if an error has occurred.
//   - Entry() returns nil if there are no more entries or if an error has been encountered.
//
// # Example
//
//	lister, err := op.List("path/to/list")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for lister.Next() {
//		entry := lister.Entry()
//		// Process the entry
//		fmt.Println(entry.Name())
//	}
type Lister struct {
	inner *opendalLister
	ctx   context.Context
	entry *Entry
	err   error
}

// This method implements the io.Closer interface. It should be called when
// the Lister is no longer needed to ensure proper resource cleanup.
func (l *Lister) Close() error {
	free := getFFI[listerFree](l.ctx, symListerFree)
	free(l.inner)

	return nil
}

func (l *Lister) Error() error {
	return l.err
}

// Next advances the Lister to the next entry in the list.
//
// This method must be called before accessing the current entry. It prepares
// the next entry for reading and indicates whether there are more entries
// to process.
//
// # Returns
//
//   - bool: true if there is another entry to process, false if the end of the list
//     has been reached or an error occurred.
//
// # Usage
//
// Next should be used in a loop condition to iterate through all entries:
//
//	for lister.Next() {
//		entry := lister.Entry()
//		// Process the entry
//	}
//
// # Error Handling
//
// If an error occurs during iteration, Next will return false. The error
// can then be retrieved by calling the Err method on the Lister.
//
// # Example
//
//	lister, err := op.List("path/to/list")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for lister.Next() {
//		entry := lister.Entry()
//		fmt.Println(entry.Name())
//	}
func (l *Lister) Next() bool {
	next := getFFI[listerNext](l.ctx, symListerNext)
	inner, err := next(l.inner)
	if inner == nil || err != nil {
		l.err = err
		l.entry = nil
		return false
	}

	entry := newEntry(l.ctx, inner)

	l.entry = entry
	return true
}

// Entry returns the current Entry in the list.
// Returns nil if there are no more entries
func (l *Lister) Entry() *Entry {
	return l.entry
}

// Entry represents a path and its associated metadata as returned by Lister.
//
// An Entry provides basic information about a file or directory encountered
// during a list operation. It contains the path of the item and minimal metadata.
//
// # Limitations
//
// The Entry itself does not contain comprehensive metadata. For detailed
// metadata information, use the op.Stat() method with the Entry's path.
//
// # Usage
//
// Entries are typically obtained through iteration of a Lister:
//
//	for lister.Next() {
//		entry := lister.Entry()
//		// Process the entry
//		fmt.Println(entry.Name())
//	}
//
// # Fetching Detailed Metadata
//
// To obtain comprehensive metadata for an Entry, use op.Stat():
//
//	meta, err := op.Stat(entry.Path())
//	if err != nil {
//		log.Printf("Error fetching metadata: %v", err)
//		return
//	}
//	fmt.Printf("Size: %d, Last Modified: %s\n", meta.ContentLength(), meta.LastModified())
//
// # Methods
//
// Entry provides methods to access basic information:
//   - Path(): Returns the full path of the entry.
//   - Name(): Returns the name of the entry (last component of the path).
type Entry struct {
	name string
	path string
}

func newEntry(ctx context.Context, inner *opendalEntry) *Entry {
	name := getFFI[entryName](ctx, symEntryName)
	path := getFFI[entryPath](ctx, symEntryPath)
	free := getFFI[entryFree](ctx, symEntryFree)

	defer free(inner)

	return &Entry{
		name: name(inner),
		path: path(inner),
	}
}

// Name returns the last component of the entry's path.
func (e *Entry) Name() string {
	return e.name
}

// Path returns the full path of the entry.
func (e *Entry) Path() string {
	return e.path
}

const symOperatorList = "opendal_operator_list"

type operatorList func(op *opendalOperator, path string) (*opendalLister, error)

var withOperatorList = withFFI(ffiOpts{
	sym:    symOperatorList,
	rType:  &typeResultList,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorList {
	return func(op *opendalOperator, path string) (*opendalLister, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result opendalResultList
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.err != nil {
			return nil, parseError(ctx, result.err)
		}
		return result.lister, nil
	}
})

const symListerFree = "opendal_lister_free"

type listerFree func(l *opendalLister)

var withListerFree = withFFI(ffiOpts{
	sym:    symListerFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) listerFree {
	return func(l *opendalLister) {
		ffiCall(
			nil,
			unsafe.Pointer(&l),
		)
	}
})

const symListerNext = "opendal_lister_next"

type listerNext func(l *opendalLister) (*opendalEntry, error)

var withListerNext = withFFI(ffiOpts{
	sym:    symListerNext,
	rType:  &typeResultListerNext,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) listerNext {
	return func(l *opendalLister) (*opendalEntry, error) {
		var result opendalResultListerNext
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&l),
		)
		if result.err != nil {
			return nil, parseError(ctx, result.err)
		}
		return result.entry, nil
	}
})

const symEntryFree = "opendal_entry_free"

type entryFree func(e *opendalEntry)

var withEntryFree = withFFI(ffiOpts{
	sym:    symEntryFree,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) entryFree {
	return func(e *opendalEntry) {
		ffiCall(
			nil,
			unsafe.Pointer(&e),
		)
	}
})

const symEntryName = "opendal_entry_name"

type entryName func(e *opendalEntry) string

var withEntryName = withFFI(ffiOpts{
	sym:    symEntryName,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) entryName {
	return func(e *opendalEntry) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&e),
		)
		return unix.BytePtrToString(bytePtr)
	}
})

const symEntryPath = "opendal_entry_path"

type entryPath func(e *opendalEntry) string

var withEntryPath = withFFI(ffiOpts{
	sym:    symEntryPath,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) entryPath {
	return func(e *opendalEntry) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&e),
		)
		return unix.BytePtrToString(bytePtr)
	}
})
