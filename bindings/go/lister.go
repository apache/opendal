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
	defer func() {
		closeErr := ds.Close()
		if err == nil {
			err = closeErr
		}
	}()
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
//			fmt.Printf("Name: %s\n", entry.Name())
//			if meta := entry.Metadata(); meta != nil {
//				fmt.Printf("Length: %d\n", meta.ContentLength())
//				fmt.Printf("Last Modified: %s\n", meta.LastModified())
//				fmt.Printf("Is Directory: %v, Is File: %v\n", meta.IsDir(), meta.IsFile())
//			}
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
	inner, err := ffiOperatorList.symbol(op.ctx)(op.inner, path)
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
	ffiListerFree.symbol(l.ctx)(l.inner)

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
	inner, err := ffiListerNext.symbol(l.ctx)(l.inner)
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
// # Usage
//
// Entries are typically obtained through iteration of a Lister:
//
//	for lister.Next() {
//		entry := lister.Entry()
//		// Process the entry
//		fmt.Println(entry.Name())
//		if m := entry.Metadata(); m != nil {
//			fmt.Printf("Size: %d\n", m.ContentLength())
//		}
//	}
//
// # Metadata
//
// Use Metadata() to retrieve metadata populated during the list operation.
//
// # Methods
//
// Entry provides the following methods:
//   - Name(): Returns the name of the entry (last component of the path).
//   - Path(): Returns the full path of the entry.
//   - Metadata(): Returns metadata collected during listing, if available.
type Entry struct {
	name string
	path string
	meta *Metadata
}

func newEntry(ctx context.Context, inner *opendalEntry) *Entry {
	name := ffiEntryName.symbol(ctx)(inner)
	path := ffiEntryPath.symbol(ctx)(inner)

	var meta *Metadata
	if m := ffiEntryMetadata.symbol(ctx)(inner); m != nil {
		meta = newMetadata(ctx, m)
	}

	ffiEntryFree.symbol(ctx)(inner)

	return &Entry{
		name: name,
		path: path,
		meta: meta,
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

// Metadata returns the metadata associated with this entry, as reported by the
// underlying storage service during the list operation.
func (e *Entry) Metadata() *Metadata {
	return e.meta
}

var ffiOperatorList = newFFI(ffiOpts{
	sym:    "opendal_operator_list",
	rType:  &typeResultList,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string) (*opendalLister, error) {
	return func(op *opendalOperator, path string) (*opendalLister, error) {
		bytePath, err := BytePtrFromString(path)
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

var ffiListerFree = newFFI(ffiOpts{
	sym:    "opendal_lister_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(l *opendalLister) {
	return func(l *opendalLister) {
		ffiCall(
			nil,
			unsafe.Pointer(&l),
		)
	}
})

var ffiListerNext = newFFI(ffiOpts{
	sym:    "opendal_lister_next",
	rType:  &typeResultListerNext,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(l *opendalLister) (*opendalEntry, error) {
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

var ffiEntryFree = newFFI(ffiOpts{
	sym:    "opendal_entry_free",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(e *opendalEntry) {
	return func(e *opendalEntry) {
		ffiCall(
			nil,
			unsafe.Pointer(&e),
		)
	}
})

var ffiEntryName = func() *FFI[func(e *opendalEntry) string] {
	_ = ffiStringFree
	return newFFI(ffiOpts{
		sym:    "opendal_entry_name",
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(ctx context.Context, ffiCall ffiCall) func(e *opendalEntry) string {
		return func(e *opendalEntry) string {
			var bytePtr *byte
			ffiCall(
				unsafe.Pointer(&bytePtr),
				unsafe.Pointer(&e),
			)
			return copyCStringAndFree(bytePtr, ffiStringFree.symbol(ctx))
		}
	})
}()

var ffiEntryPath = func() *FFI[func(e *opendalEntry) string] {
	_ = ffiStringFree
	return newFFI(ffiOpts{
		sym:    "opendal_entry_path",
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(ctx context.Context, ffiCall ffiCall) func(e *opendalEntry) string {
		return func(e *opendalEntry) string {
			var bytePtr *byte
			ffiCall(
				unsafe.Pointer(&bytePtr),
				unsafe.Pointer(&e),
			)
			return copyCStringAndFree(bytePtr, ffiStringFree.symbol(ctx))
		}
	})
}()

var ffiEntryMetadata = newFFI(ffiOpts{
	sym:    "opendal_entry_metadata",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(e *opendalEntry) *opendalMetadata {
	return func(e *opendalEntry) *opendalMetadata {
		var meta *opendalMetadata
		ffiCall(
			unsafe.Pointer(&meta),
			unsafe.Pointer(&e),
		)
		return meta
	}
})
