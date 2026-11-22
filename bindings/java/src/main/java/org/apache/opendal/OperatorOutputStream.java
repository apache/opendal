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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class OperatorOutputStream extends OutputStream {
    private static class Writer extends NativeObject {
        private Writer(long nativeHandle) {
            super(nativeHandle);
        }

        @Override
        protected void disposeInternal(long handle) {
            disposeWriter(handle);
        }
    }

    private static final int DEFAULT_MAX_BYTES = 16384;
    private static final int MAX_DIRECT_WRITE_BYTES = 1024 * 1024;

    private final Writer writer;
    private final byte[] bytes;
    private final int maxBatchBytes;

    private int offset = 0;

    public OperatorOutputStream(Operator operator, String path, WriteOptions options) {
        this(operator, path, DEFAULT_MAX_BYTES, MAX_DIRECT_WRITE_BYTES, options);
    }

    /**
     * Creates a stream with the given buffer size and maximum native write size.
     *
     * @throws IllegalArgumentException if maxBytes is not positive
     */
    public OperatorOutputStream(Operator operator, String path, int maxBytes, WriteOptions options) {
        this(operator, path, maxBytes, maxBytes, options);
    }

    private OperatorOutputStream(
            Operator operator, String path, int maxBytes, int maxBatchBytes, WriteOptions options) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive");
        }
        this.writer = new Writer(operator.withHandle(handle -> constructWriter(handle, path, options)));
        this.bytes = new byte[maxBytes];
        this.maxBatchBytes = maxBatchBytes;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        ensureOpen();
        bytes[offset++] = (byte) b;
        if (offset == bytes.length) {
            flush();
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        Objects.requireNonNull(b);
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();
        while (len > 0) {
            final int count;
            if (offset == 0 && len >= bytes.length) {
                count = Math.min(len, maxBatchBytes);
                writeByteRange(writer.nativeHandle, b, off, count);
            } else {
                count = Math.min(len, bytes.length - offset);
                System.arraycopy(b, off, bytes, offset, count);
                offset += count;
                if (offset == bytes.length) {
                    flush();
                }
            }
            off += count;
            len -= count;
        }
    }

    @Override
    public synchronized void flush() {
        ensureOpen();
        if (offset != 0) {
            writeByteRange(writer.nativeHandle, bytes, 0, offset);
            offset = 0;
        }
    }

    @Override
    public synchronized void close() {
        if (!writer.isDisposed()) {
            flush();
            writer.close();
        }
    }

    private void ensureOpen() {
        if (writer.isDisposed()) {
            throw new IllegalStateException("OperatorOutputStream is closed");
        }
    }

    private static native long constructWriter(long op, String path, WriteOptions options);

    private static native void disposeWriter(long writer);

    private static native void writeByteRange(long writer, byte[] bytes, int off, int len);
}
