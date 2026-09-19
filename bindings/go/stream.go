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

// These compile-time interface checks detect changes that break the stream copy interfaces.
var _ io.ReaderFrom = (*Writer)(nil)
var _ io.WriterTo = (*Reader)(nil)

// ReadFrom copies data from src into the Writer. It stops at EOF or an error.
// ReadFrom returns the number of bytes copied. At EOF, ReadFrom returns a nil error.
// ReadFrom uses one 256 KiB buffer for the copy.
//
// ReadFrom leaves both streams open.
// The caller must call Writer.Close to complete the write and get its metadata.
func (w *Writer) ReadFrom(src io.Reader) (int64, error) {
	return io.CopyBuffer(struct{ io.Writer }{w}, struct{ io.Reader }{src}, make([]byte, streamCopyBufferSize))
}

// WriteTo copies data from the Reader into dst. It stops at EOF or an error.
// WriteTo returns the number of bytes copied. At EOF, WriteTo returns a nil error.
// WriteTo uses one 256 KiB buffer for the copy.
//
// WriteTo writes the bytes from each Read call before it reads more data.
// WriteTo leaves both streams open. The caller must close each stream.
func (r *Reader) WriteTo(dst io.Writer) (int64, error) {
	return io.CopyBuffer(struct{ io.Writer }{dst}, struct{ io.Reader }{r}, make([]byte, streamCopyBufferSize))
}
