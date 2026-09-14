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

package org.apache.opendal;

import java.io.InputStream;
import java.util.Objects;

/**
 * Reads a byte range sequentially through an {@link OperatorReader}.
 * Each stream owns its native iterator and must be closed independently of its source reader.
 * Reading a closed stream throws {@link IllegalStateException}.
 */
public class OperatorInputStream extends InputStream {
    private static class BytesIterator extends NativeObject {
        private BytesIterator(long nativeHandle) {
            super(nativeHandle);
        }

        @Override
        protected void disposeInternal(long handle) {
            disposeReader(handle);
        }
    }

    private final BytesIterator reader;

    private int offset = 0;
    private byte[] bytes = new byte[0];

    OperatorInputStream(long nativeHandle) {
        this.reader = new BytesIterator(nativeHandle);
    }

    public OperatorInputStream(Operator operator, String path, ReadOptions options) {
        this(operator, path, options, ReaderOptions.builder().build());
    }

    /**
     * Creates a stream with a logical range and independent execution controls.
     *
     * @param operator operator that reads the object
     * @param path object path
     * @param readOptions logical offset and length
     * @param readerOptions internal chunk request and buffering controls
     * @throws OpenDALException if reader options are invalid (ConfigInvalid) or creation fails
     */
    public OperatorInputStream(Operator operator, String path, ReadOptions readOptions, ReaderOptions readerOptions) {
        Objects.requireNonNull(readOptions, "readOptions");
        try (OperatorReader source = operator.reader(path, readerOptions)) {
            this.reader = new BytesIterator(source.createBytesIterator(readOptions.offset, readOptions.length));
        }
    }

    @Override
    public synchronized int read() {
        if (reader.isDisposed()) {
            throw new IllegalStateException("OperatorInputStream is closed");
        }
        while (bytes != null && offset >= bytes.length) {
            bytes = readNextBytes(reader.nativeHandle);
            offset = 0;
        }

        if (bytes != null) {
            return bytes[offset++] & 0xFF;
        }

        return -1;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
        Objects.requireNonNull(b);
        if ((b.length | off | len) < 0 || len > b.length - off) {
            // Objects.checkFromIndexSize has only been available since Java 9
            throw new IndexOutOfBoundsException(
                    String.format("Range [%s, %<s + %s) out of bounds for length %s", off, len, b.length));
        }
        if (reader.isDisposed()) {
            throw new IllegalStateException("OperatorInputStream is closed");
        }
        int read = 0;
        while (len > 0) {
            while (bytes != null && offset >= bytes.length) {
                bytes = readNextBytes(reader.nativeHandle);
                offset = 0;
            }

            if (bytes == null) {
                return read != 0 ? read : -1;
            }

            final int n = Math.min(len, bytes.length - offset);
            System.arraycopy(bytes, offset, b, off, n);
            offset += n;
            off += n;
            read += n;
            len -= n;
        }

        return read;
    }

    @Override
    public synchronized void close() {
        reader.close();
        bytes = null;
    }

    private static native void disposeReader(long reader);

    private static native byte[] readNextBytes(long reader);
}
