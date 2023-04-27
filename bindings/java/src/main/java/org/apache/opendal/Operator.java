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
        this.ptr = getOperator(schema, params);
    }

    public void write(String fileName, String content) {
        write(this.ptr, fileName, content);
    }

    public CompletableFuture<Boolean> writeAsync(String fileName, String content) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        writeAsync(this.ptr, fileName, content, future);
        return future;
    }

    public String read(String s) {
        return read(this.ptr, s);
    }

    public void delete(String s) {
        delete(this.ptr, s);
    }

    public Metadata stat(String fileName) {
        long statPtr = stat(this.ptr, fileName);
        return new Metadata(statPtr);
    }

    @Override
    public void close() {
        this.freeOperator(ptr);
    }

    private native long getOperator(String type, Map<String, String> params);

    protected native void freeOperator(long ptr);

    private native void write(long ptr, String fileName, String content);

    private native void writeAsync(long ptr, String fileName, String content, CompletableFuture<Boolean> future);

    private native String read(long ptr, String fileName);

    private native void delete(long ptr, String fileName);

    private native long stat(long ptr, String file);
}
