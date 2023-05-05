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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Operator extends OpenDALObject {
    public Operator(String schema, Map<String, String> params) {
        Utils.checkNotBlank(schema, "schema");
        Utils.checkNotNull(params, "params");

        long ptr = getOperator(schema, params);
        Utils.checkNullPointer(ptr);
        this.ptr = ptr;
    }

    public void write(String fileName, byte[] content) {
        Utils.checkNotBlank(fileName, "fileName");
        Utils.checkNotNull(content, "content");

        write(this.ptr, fileName, content);
    }

    public CompletableFuture<Boolean> writeAsync(String fileName, byte[] content) {
        Utils.checkNotBlank(fileName, "fileName");
        Utils.checkNotNull(content, "content");

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        writeAsync(this.ptr, fileName, content, future);
        return future;
    }

    public byte[] read(String file) {
        Utils.checkNotBlank(file, "file");

        return read(this.ptr, file);
    }

    public void delete(String file) {
        Utils.checkNotBlank(file, "file");

        delete(this.ptr, file);
    }

    public Metadata stat(String file) {
        Utils.checkNotBlank(file, "file");

        long metadataPtr = stat(this.ptr, file);
        return new Metadata(metadataPtr);
    }

    @Override
    public void close() {
        if (this.ptr != 0) {
            this.freeOperator(this.ptr);
        }
    }

    private native long getOperator(String type, Map<String, String> params);

    protected native void freeOperator(long ptr);

    private native void write(long ptr, String fileName, byte[] content);

    private native void writeAsync(long ptr, String fileName, byte[] content, CompletableFuture<Boolean> future);

    private native byte[] read(long ptr, String fileName);

    private native void delete(long ptr, String fileName);

    private native long stat(long ptr, String file);
}
