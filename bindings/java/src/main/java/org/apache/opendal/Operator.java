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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Operator represents an underneath OpenDAL operator that
 * accesses data asynchronously.
 */
public class Operator extends NativeObject {

    /**
     * Singleton to hold all outstanding futures.
     *
     * <p>
     * This is a trick to avoid using global references to pass {@link CompletableFuture}
     * among language boundary and between multiple native threads.
     *
     * @see <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/functions.html#global_references">Global References</a>
     * @see <a href="https://docs.rs/jni/latest/jni/objects/struct.GlobalRef.html">jni::objects::GlobalRef</a>
     */
    private enum AsyncRegistry {
        INSTANCE;

        private final Map<Long, CompletableFuture<?>> registry = new ConcurrentHashMap<>();

        /**
         * Request a new {@link CompletableFuture} that is associated with a unique ID.
         *
         * <p>
         * This method is called from native code. The return ID is used by:
         *
         * <li>Rust side: {@link #get(long)} the future when the native async op completed</li>
         * <li>Java side: {@link #take(long)} the future to compose with more actions</li>
         *
         * @return the request ID associated to the obtained future
         */
        @SuppressWarnings("unused")
        private static long requestId() {
            final CompletableFuture<?> f = new CompletableFuture<>();
            while (true) {
                final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
                final CompletableFuture<?> prev = INSTANCE.registry.putIfAbsent(requestId, f);
                if (prev == null) {
                    return requestId;
                }
            }
        }

        /**
         * Get the future associated with the request ID.
         *
         * <p>
         * This method is called from native code.
         *
         * @param requestId to identify the future
         * @return the future associated with the request ID
         */
        private static CompletableFuture<?> get(long requestId) {
            return INSTANCE.registry.get(requestId);
        }

        /**
         * Take the future associated with the request ID.
         *
         * @param requestId to identify the future
         * @return the future associated with the request ID
         */
        @SuppressWarnings("unchecked")
        private static <T> CompletableFuture<T> take(long requestId) {
            final CompletableFuture<?> f = get(requestId);
            if (f != null) {
                f.whenComplete((r, e) -> INSTANCE.registry.remove(requestId));
            }
            return (CompletableFuture<T>) f;
        }
    }

    public final OperatorInfo info;

    private final AsyncExecutor executor;

    /**
     * Construct an OpenDAL operator:
     *
     * <p>
     * You can find all possible schemes <a href="https://docs.rs/opendal/latest/opendal/enum.Scheme.html">here</a>
     * and see what config options each service supports.
     *
     * @param schema the name of the underneath service to access data from.
     * @param map    a map of properties to construct the underneath operator.
     */
    public static Operator of(String schema, Map<String, String> map) {
        return of(schema, map, null);
    }

    public static Operator of(String schema, Map<String, String> map, AsyncExecutor executor) {
        final long nativeHandle = constructor(schema, map);
        final OperatorInfo info = makeOperatorInfo(nativeHandle);
        return new Operator(nativeHandle, info, executor);
    }

    private Operator(long nativeHandle, OperatorInfo info, AsyncExecutor executor) {
        super(nativeHandle);
        this.info = info;
        this.executor = executor;
    }

    /**
     * Clone a new operator that is identical to this one. The new operator has its own lifecycle.
     *
     * <p>Since an operator will release all its resource and "flush" on lifecycle end, this method
     * is suitable to create a narrowed "scope" while avoiding creating a brand-new operator for each
     * scope.
     *
     * @return the cloned operator.
     */
    public Operator duplicate() {
        final long nativeHandle = duplicate(this.nativeHandle);
        return new Operator(nativeHandle, this.info, this.executor);
    }

    public Operator layer(Layer layer) {
        final long nativeHandle = layer.layer(this.nativeHandle);
        return new Operator(nativeHandle, makeOperatorInfo(nativeHandle), this.executor);
    }

    public BlockingOperator blocking() {
        final long nativeHandle = makeBlockingOp(this.nativeHandle);
        final OperatorInfo info = this.info;
        return new BlockingOperator(nativeHandle, info);
    }

    public CompletableFuture<Void> write(String path, String content) {
        return write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    public CompletableFuture<Void> write(String path, byte[] content) {
        final long requestId = write(nativeHandle, executorHandle(), path, content);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> append(String path, String content) {
        return append(path, content.getBytes(StandardCharsets.UTF_8));
    }

    public CompletableFuture<Void> append(String path, byte[] content) {
        final long requestId = append(nativeHandle, executorHandle(), path, content);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Metadata> stat(String path) {
        final long requestId = stat(nativeHandle, executorHandle(), path);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<byte[]> read(String path) {
        final long requestId = read(nativeHandle, executorHandle(), path);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<PresignedRequest> presignRead(String path, Duration duration) {
        final long requestId = presignRead(nativeHandle, executorHandle(), path, duration.toNanos());
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<PresignedRequest> presignWrite(String path, Duration duration) {
        final long requestId = presignWrite(nativeHandle, executorHandle(), path, duration.toNanos());
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<PresignedRequest> presignStat(String path, Duration duration) {
        final long requestId = presignStat(nativeHandle, executorHandle(), path, duration.toNanos());
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> delete(String path) {
        final long requestId = delete(nativeHandle, executorHandle(), path);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> createDir(String path) {
        final long requestId = createDir(nativeHandle, executorHandle(), path);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> copy(String sourcePath, String targetPath) {
        final long requestId = copy(nativeHandle, executorHandle(), sourcePath, targetPath);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> rename(String sourcePath, String targetPath) {
        final long requestId = rename(nativeHandle, executorHandle(), sourcePath, targetPath);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> removeAll(String path) {
        final long requestId = removeAll(nativeHandle, executorHandle(), path);
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<List<Entry>> list(String path) {
        final long requestid = list(nativeHandle, executorHandle(), path);
        final CompletableFuture<Entry[]> result = AsyncRegistry.take(requestid);
        return Objects.requireNonNull(result).thenApplyAsync(Arrays::asList);
    }

    private long executorHandle() {
        return this.executor != null ? this.executor.nativeHandle : 0;
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long duplicate(long nativeHandle);

    private static native long constructor(String schema, Map<String, String> map);

    private static native long read(long nativeHandle, long executorHandle, String path);

    private static native long write(long nativeHandle, long executorHandle, String path, byte[] content);

    private static native long append(long nativeHandle, long executorHandle, String path, byte[] content);

    private static native long delete(long nativeHandle, long executorHandle, String path);

    private static native long stat(long nativeHandle, long executorHandle, String path);

    private static native long presignRead(long nativeHandle, long executorHandle, String path, long duration);

    private static native long presignWrite(long nativeHandle, long executorHandle, String path, long duration);

    private static native long presignStat(long nativeHandle, long executorHandle, String path, long duration);

    private static native OperatorInfo makeOperatorInfo(long nativeHandle);

    private static native long makeBlockingOp(long nativeHandle);

    private static native long createDir(long nativeHandle, long executorHandle, String path);

    private static native long copy(long nativeHandle, long executorHandle, String sourcePath, String targetPath);

    private static native long rename(long nativeHandle, long executorHandle, String sourcePath, String targetPath);

    private static native long removeAll(long nativeHandle, long executorHandle, String path);

    private static native long list(long nativeHandle, long executorHandle, String path);
}
