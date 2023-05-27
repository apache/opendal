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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class Operator extends NativeObject {
    private static AsyncRegistry registry() {
        return AsyncRegistry.INSTANCE;
    }

    private enum AsyncRegistry {
        INSTANCE;

        private final Map<Long, CompletableFuture<?>> registry = new ConcurrentHashMap<>();

        @SuppressWarnings("unused") // called by jni-rs
        private long requestId() {
            final CompletableFuture<?> f = new CompletableFuture<>();
            while (true) {
                final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
                final CompletableFuture<?> prev = registry.putIfAbsent(requestId, f);
                if (prev == null) {
                    return requestId;
                }
            }
        }

        private CompletableFuture<?> get(long requestId) {
            return registry.get(requestId);
        }

        @SuppressWarnings("unchecked")
        private <T> CompletableFuture<T> take(long requestId) {
            final CompletableFuture<?> f = get(requestId);
            if (f != null) {
                f.whenComplete((r, e) -> registry.remove(requestId));
            }
            return (CompletableFuture<T>) f;
        }
    }

    public Operator(String schema, Map<String, String> map) {
        super(constructor(schema, map));
    }

    public CompletableFuture<Void> write(String path, String content) {
        final long requestId = write(nativeHandle, path, content);
        return registry().take(requestId);
    }

    public CompletableFuture<Metadata> stat(String path) {
        final long requestId = stat(nativeHandle, path);
        final CompletableFuture<Long> f = registry().take(requestId);
        return f.thenApply(Metadata::new);
    }

    public CompletableFuture<String> read(String path) {
        final long requestId = read(nativeHandle, path);
        return registry().take(requestId);
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long constructor(String schema, Map<String, String> map);

    private static native long read(long nativeHandle, String path);

    private static native long write(long nativeHandle, String path, String content);

    private static native long stat(long nativeHandle, String file);
}
