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
import java.util.Arrays;

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

    private static final int MAX_BYTES = 16384;

    private final Writer writer;
    private final byte[] bytes = new byte[MAX_BYTES];

    private int offset = 0;

    public OperatorOutputStream(Operator operator, String path) {
        final long op = operator.nativeHandle;
        this.writer = new Writer(constructWriter(op, path));
    }

    @Override
    public void write(int b) throws IOException {
        bytes[offset++] = (byte) b;
        if (offset >= MAX_BYTES) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        if (offset > MAX_BYTES) {
            throw new IOException("INTERNAL ERROR: " + offset + " > " + MAX_BYTES);
        } else if (offset < MAX_BYTES) {
            final byte[] bytes = Arrays.copyOf(this.bytes, offset);
            writeBytes(writer.nativeHandle, bytes);
        } else {
            writeBytes(writer.nativeHandle, bytes);
        }
        offset = 0;
    }

    @Override
    public void close() throws IOException {
        flush();
        writer.close();
    }

    private static native long constructWriter(long op, String path);

    private static native long disposeWriter(long writer);

    private static native byte[] writeBytes(long writer, byte[] bytes);
}
