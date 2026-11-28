// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package opendal

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The list options released before options.go was generated from core.
// The generator must keep each of them with this exact signature.
var (
	_ func(*listOptions)                 = WithListFn(nil)
	_ func(recursive bool) WithListFn    = ListWithRecursive
	_ func(limit uint) WithListFn        = ListWithLimit
	_ func(startAfter string) WithListFn = ListWithStartAfter
	_ func(versions bool) WithListFn     = ListWithVersions
	_ func(deleted bool) WithListFn      = ListWithDeleted
)

// TestListOptionsExportedSet fails when the generator adds or drops a list
// option, such as exposing `glob` as ListWithGlob.
func TestListOptionsExportedSet(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var options []string
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), source, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "ListWith") {
				options = append(options, fn.Name.Name)
			}
		}
	}
	sort.Strings(options)
	want := []string{"ListWithDeleted", "ListWithLimit", "ListWithRecursive", "ListWithStartAfter", "ListWithVersions"}
	if !slices.Equal(options, want) {
		t.Fatalf("exported list options = %v, want %v", options, want)
	}
}

// TestListOptionsOnMemory lists a memory operator with each list option
// through the live C library.
func TestListOptionsOnMemory(t *testing.T) {
	ctx := context.Background()
	op, err := NewOperator(localTestScheme{name: "memory", path: findLocalCLibrary(t)}, OperatorOptions{"root": "/list-options"})
	if err != nil {
		t.Fatal(err)
	}
	defer op.Close()
	for _, path := range []string{"dir/a", "dir/b", "dir/sub/c"} {
		if _, err := op.Write(ctx, path, []byte(path)); err != nil {
			t.Fatalf("Write(%q) = %v", path, err)
		}
	}

	list := func(opts ...WithListFn) ([]string, error) {
		lister, err := op.List(ctx, "dir/", opts...)
		if err != nil {
			return nil, err
		}
		var paths []string
		for lister.Next(ctx) {
			paths = append(paths, lister.Entry().Path())
		}
		sort.Strings(paths)
		return paths, errors.Join(lister.Error(), lister.Close())
	}

	for _, tc := range []struct {
		name string
		opts []WithListFn
		want []string
	}{
		{"none", nil, []string{"dir/a", "dir/b", "dir/sub/"}},
		{"recursive", []WithListFn{ListWithRecursive(true)}, []string{"dir/a", "dir/b", "dir/sub/c"}},
		{"not_recursive", []WithListFn{ListWithRecursive(true), ListWithRecursive(false)}, []string{"dir/a", "dir/b", "dir/sub/"}},
		// limit is a per-request hint, so the listing stays complete.
		{"limit", []WithListFn{ListWithLimit(1)}, []string{"dir/a", "dir/b", "dir/sub/"}},
		{"start_after", []WithListFn{ListWithStartAfter("dir/a")}, []string{"dir/b", "dir/sub/"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths, err := list(tc.opts...)
			if err != nil || !reflect.DeepEqual(paths, tc.want) {
				t.Fatalf("List(dir/) = %v, %v, want %v, nil", paths, err, tc.want)
			}
		})
	}

	// Memory lacks Capability.ListWithVersions and Capability.ListWithDeleted.
	for _, tc := range []struct {
		name string
		opt  WithListFn
	}{
		{"versions", ListWithVersions(true)},
		{"deleted", ListWithDeleted(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths, err := list(tc.opt)
			var opendalErr *Error
			if !errors.As(err, &opendalErr) || opendalErr.Code() != CodeUnsupported {
				t.Fatalf("List(dir/) = %v, %v, want Unsupported error", paths, err)
			}
		})
	}
}

// TestListWithLimitReachesC covers the option that memory ignores: a limit
// other than 0 reaches opendal_list_options_set_limit, and 0 leaves it unset.
func TestListWithLimitReachesC(t *testing.T) {
	for _, tc := range []struct {
		limit uint
		want  []uint
	}{{0, nil}, {7, []uint{7}}} {
		var got []uint
		ctx := context.Background()
		ctx = context.WithValue(ctx, ffiListOptionsNew.sym, func() *opendalListOptions { return &opendalListOptions{} })
		ctx = context.WithValue(ctx, ffiListOptionsSetRecursive.sym, func(*opendalListOptions, bool) {})
		ctx = context.WithValue(ctx, ffiListOptionsSetVersions.sym, func(*opendalListOptions, bool) {})
		ctx = context.WithValue(ctx, ffiListOptionsSetDeleted.sym, func(*opendalListOptions, bool) {})
		ctx = context.WithValue(ctx, ffiListOptionsSetLimit.sym, func(_ *opendalListOptions, limit uint) {
			got = append(got, limit)
		})
		if _, err := newOpendalListOptions(ctx, parseListOptions(ListWithLimit(tc.limit))); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("ListWithLimit(%d) set limits %v, want %v", tc.limit, got, tc.want)
		}
	}
}
