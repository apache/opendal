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
import java.io.InputStream;

public class OperatorInputStream extends InputStream {
    private static class Reader extends NativeObject {
        private Reader(long nativeHandle) {
            super(nativeHandle);
        }

        @Override
        protected void disposeInternal(long handle) {
            disposeReader(handle);
        }
    }

    private final Reader reader;

    private int offset = 0;
    private byte[] bytes = new byte[0];

    public OperatorInputStream(BlockingOperator operator, String path) {
        final long op = operator.nativeHandle;
        this.reader = new Reader(constructReader(op, path));
    }

    @Override
    public int read() throws IOException {
        if (bytes != null && offset >= bytes.length) {
            bytes = readNextBytes(reader.nativeHandle);
            offset = 0;
        }

        if (bytes != null) {
            return bytes[offset++] & 0xFF;
        }

        return -1;
    }

    @Override
    public void close() throws IOException {
        reader.close();
        super.close();
    }

    private static native long constructReader(long op, String path);

    private static native long disposeReader(long reader);

    private static native byte[] readNextBytes(long reader);
}
