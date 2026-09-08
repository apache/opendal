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

import "io"

const streamCopyBufferSize = 256 * 1024

// Go matches a type to an interface by its method signatures.
// These assignments check that match at compile time.
// A missing method or a different signature causes a compile error.
// The checks use typed nil pointers, so they do not create Reader or Writer instances.
// The blank identifier (_) discards the values.
var _ io.ReaderFrom = (*Writer)(nil)
var _ io.WriterTo = (*Reader)(nil)

// ReadFrom copies data from src into the Writer. It stops at EOF or an error.
// ReadFrom returns the number of bytes copied. At EOF, ReadFrom returns a nil error.
// ReadFrom uses one 256 KiB buffer for the copy.
//
// ReadFrom leaves both streams open.
// The caller must call Writer.Close to complete the write and get its metadata.
func (w *Writer) ReadFrom(src io.Reader) (int64, error) {
	// CopyBuffer checks for WriterTo and ReaderFrom before it uses the buffer.
	// Each wrapper embeds an interface and exposes only Read or Write.
	// CopyBuffer cannot see ReadFrom or src.WriteTo through these wrappers.
	// Thus, CopyBuffer uses this buffer and does not call either copy method.
	return io.CopyBuffer(struct{ io.Writer }{w}, struct{ io.Reader }{src}, make([]byte, streamCopyBufferSize))
}

// WriteTo copies data from the Reader into dst. It stops at EOF or an error.
// WriteTo returns the number of bytes copied. At EOF, WriteTo returns a nil error.
// WriteTo uses one 256 KiB buffer for the copy.
//
// WriteTo writes the bytes from each Read call before it reads more data.
// WriteTo leaves both streams open. The caller must close each stream.
func (r *Reader) WriteTo(dst io.Writer) (int64, error) {
	// As in ReadFrom, the wrappers expose only Read and Write.
	// CopyBuffer uses this buffer and cannot call WriteTo again or use dst.ReadFrom.
	return io.CopyBuffer(struct{ io.Writer }{dst}, struct{ io.Reader }{r}, make([]byte, streamCopyBufferSize))
}
