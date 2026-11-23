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
        /**
         * Whether disposing this object commits it. Guarded by the owning
         * {@link OperatorOutputStream}, which owns the only reference.
         */
        private boolean commitOnDispose = true;

        private Writer(long nativeHandle) {
            super(nativeHandle);
        }

        /**
         * Releases the native writer without committing the object it was writing.
         */
        private void discard() {
            commitOnDispose = false;
            close();
        }

        @Override
        protected void disposeInternal(long handle) {
            disposeWriter(handle, commitOnDispose);
        }
    }

    private static final int DEFAULT_MAX_BYTES = 16384;
    private static final int MAX_DIRECT_WRITE_BYTES = 1024 * 1024;

    private final Writer writer;
    private final byte[] bytes;
    private final int maxBatchBytes;

    private int offset = 0;

    /**
     * The first write or flush failure, if any, including a failure thrown by a subclass
     * override of {@link #flush()} while {@link #write} ascends through it.
     *
     * <p>OpenDAL publishes an object when the native writer is closed. After a failure the native
     * writer may hold only a prefix of the content, so {@link #close()} discards the writer instead
     * of committing a truncated object.
     */
    private Throwable failure;

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
            try {
                flush();
            } catch (Throwable t) {
                recordFailure(t);
                OperatorOutputStream.<RuntimeException>sneakyThrow(t);
            }
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
                writeNative(b, off, count);
            } else {
                count = Math.min(len, bytes.length - offset);
                System.arraycopy(b, off, bytes, offset, count);
                offset += count;
                if (offset == bytes.length) {
                    try {
                        flush();
                    } catch (Throwable t) {
                        recordFailure(t);
                        OperatorOutputStream.<RuntimeException>sneakyThrow(t);
                    }
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
            flushBuffer();
        }
    }

    @Override
    public synchronized void close() {
        if (writer.isDisposed()) {
            return;
        }
        // A failure recorded by an earlier write() or flush() is already propagating from
        // the caller, possibly from the try-with-resources body of this very close() call.
        final Throwable priorFailure = failure;
        Throwable primary = priorFailure;
        if (priorFailure == null) {
            try {
                flush();
            } catch (Throwable t) {
                primary = t;
            }
        }
        // Release the native writer even when the stream failed, otherwise the handle
        // leaks and the stream keeps accepting writes. A failed stream is discarded
        // instead of closed, because closing would commit the bytes that did reach the
        // native writer, possibly truncated. `primary` also covers first failures that
        // never reached `writeNative`, such as a subclass override of `flush()` or a JNI
        // exception that is neither a RuntimeException nor an Error.
        Throwable secondary = null;
        try {
            if (primary == null) {
                writer.close();
            } else {
                writer.discard();
            }
        } catch (Throwable t) {
            secondary = t;
        }
        if (primary == null) {
            primary = secondary;
            secondary = null;
        }
        if (primary == null) {
            return;
        }
        if (priorFailure == null) {
            // close() is the first caller to observe this failure, so nobody is propagating
            // it yet. Rethrow it directly instead of wrapping it: the exact OpenDALException,
            // Error, or RuntimeException subclass carries the most useful context and cannot
            // trigger self-suppression.
            if (secondary != null && secondary != primary) {
                primary.addSuppressed(secondary);
            }
            OperatorOutputStream.<RuntimeException>sneakyThrow(primary);
            return;
        }
        // The caller may already be propagating this exact instance, and try-with-resources
        // suppresses whatever close() throws into it. The JVM rejects self-suppression with
        // IllegalArgumentException, so raise a fresh exception and attach the discard failure
        // to that new exception instead of the one already in flight.
        final Throwable reopened = unrecovered(priorFailure);
        if (secondary != null && secondary != reopened) {
            reopened.addSuppressed(secondary);
        }
        OperatorOutputStream.<RuntimeException>sneakyThrow(reopened);
    }

    private void flushBuffer() {
        // Clear the buffered range only after the native write succeeded, so a failed
        // flush() keeps the bytes instead of dropping them silently.
        writeNative(bytes, 0, offset);
        offset = 0;
    }

    private void writeNative(byte[] source, int off, int len) {
        try {
            writeByteRange(writer.nativeHandle, source, off, len);
        } catch (Throwable t) {
            recordFailure(t);
            OperatorOutputStream.<RuntimeException>sneakyThrow(t);
        }
    }

    /** Keeps the first failure and never clears it, so later calls reject the stream. */
    private void recordFailure(Throwable t) {
        if (failure == null) {
            failure = t;
        }
    }

    /**
     * Throws an exception for a failure that did not commit the object.
     *
     * <p>The original throwable may already be the exception a try-with-resources statement is
     * propagating, and that statement suppresses whatever {@link #close()} throws into it.
     * Suppressing an exception into itself makes the JVM throw
     * {@link IllegalArgumentException}, which would replace the real failure, so raise a fresh
     * exception that keeps the original one as its cause.
     */
    private static void throwUnrecovered(Throwable failure) {
        OperatorOutputStream.<RuntimeException>sneakyThrow(unrecovered(failure));
    }

    private static Throwable unrecovered(Throwable failure) {
        if (failure instanceof Error) {
            return new Error("OperatorOutputStream failed, the object was not committed", failure);
        }
        if (failure instanceof OpenDALException) {
            final OpenDALException original = (OpenDALException) failure;
            final OpenDALException reopened = new OpenDALException(original.getCode(), original.getMessage());
            reopened.initCause(original);
            return reopened;
        }
        return new IllegalStateException("OperatorOutputStream failed, the object was not committed", failure);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }

    private void ensureOpen() {
        if (writer.isDisposed()) {
            throw new IllegalStateException("OperatorOutputStream is closed");
        }
        // The native writer can no longer receive the buffered bytes, so report the sticky
        // failure instead of buffering writes that will never be sent. Checking the failure
        // also keeps a full buffer from overrunning its array when a failed flush left
        // `offset` at the end of the buffer.
        if (failure != null) {
            throwUnrecovered(failure);
        }
    }

    private static native long constructWriter(long op, String path, WriteOptions options);

    private static native void disposeWriter(long writer, boolean commit);

    private static native void writeByteRange(long writer, byte[] bytes, int off, int len);
}
