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
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * AsyncOperator represents an underneath OpenDAL operator that
 * accesses data asynchronously.
 *
 * <p>Operators without an explicit executor share a default executor. Operators,
 * derived streams, and outstanding operations keep it alive. Closing the last
 * operator and stream releases it after outstanding operations complete; a later
 * operator creates a new default executor.</p>
 *
 * <p>The default executor uses the same worker context class loader behavior as {@link AsyncExecutor}.</p>
 *
 * <p>If an asynchronous operation panics on the native executor, its future completes exceptionally
 * with an {@link OpenDALPanicException}. A panic during the native call itself is thrown directly.</p>
 */
public class AsyncOperator extends NativeObject {

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
     * Construct an OpenDAL operator.
     *
     * @param config the config of the underneath service to access data from.
     */
    public static AsyncOperator of(ServiceConfig config) {
        return of(config, null);
    }

    /**
     * Construct an OpenDAL operator.
     *
     * @param executor the underneath executor to run async operations; {@code null} to use a default global executor.
     */
    public static AsyncOperator of(ServiceConfig config, AsyncExecutor executor) {
        final String scheme = config.scheme();
        final Map<String, String> map = config.configMap();
        return of(scheme, map, executor);
    }

    /**
     * Construct an OpenDAL operator:
     *
     * <p>
     * You can find all possible schemes <a href="https://docs.rs/opendal/latest/opendal/enum.Scheme.html">here</a>
     * and see what config options each service supports.
     *
     * @param scheme the name of the underneath service to access data from.
     * @param map    a map of properties to construct the underneath operator.
     */
    public static AsyncOperator of(String scheme, Map<String, String> map) {
        return of(scheme, map, null);
    }

    /**
     * Construct an OpenDAL operator:
     *
     * <p>
     * You can find all possible schemes <a href="https://docs.rs/opendal/latest/opendal/enum.Scheme.html">here</a>
     * and see what config options each service supports.
     *
     * @param scheme   the name of the underneath service to access data from.
     * @param map      a map of properties to construct the underneath operator.
     * @param executor the underneath executor to run async operations; {@code null} to use a default global executor.
     */
    public static AsyncOperator of(String scheme, Map<String, String> map, AsyncExecutor executor) {
        final long nativeHandle = withExecutor(executor, handle -> constructor(scheme, map, handle));
        final OperatorInfo info = makeOperatorInfo(nativeHandle);
        return new AsyncOperator(nativeHandle, executor, info);
    }

    private AsyncOperator(long nativeHandle, AsyncExecutor executor, OperatorInfo info) {
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
    public AsyncOperator duplicate() {
        final long nativeHandle = withHandle(AsyncOperator::duplicate);
        return new AsyncOperator(nativeHandle, executor, info);
    }

    /**
     * Create a new operator that is layered with the given layer.
     *
     * <p>Note that the current operator is not modified. The returned operator is a new instance.
     * You must close the current operator on demand to avoid resource leak.
     *
     * @param layer the layer to be applied.
     *
     * @return the layered new operator.
     */
    public AsyncOperator layer(Layer layer) {
        final long nativeHandle = withHandle(handle -> layer.layer(handle));
        return new AsyncOperator(nativeHandle, executor, makeOperatorInfo(nativeHandle));
    }

    /**
     * Create a new operator that performs every operation blocking.
     *
     * <p>Note that the current operator is not modified. The returned operator is a new instance.
     * You must close the current operator on demand to avoid resource leak.
     *
     * @return the blocking new operator.
     */
    public Operator blocking() {
        final long nativeHandle = withOperatorAndExecutor(AsyncOperator::makeBlockingOp);
        return new Operator(nativeHandle, info);
    }

    public CompletableFuture<Void> write(String path, String content) {
        return write(
                path,
                content.getBytes(StandardCharsets.UTF_8),
                WriteOptions.builder().build());
    }

    public CompletableFuture<Void> write(String path, byte[] content) {
        return write(path, content, WriteOptions.builder().build());
    }

    public CompletableFuture<Void> write(String path, String content, WriteOptions options) {
        return write(path, content.getBytes(StandardCharsets.UTF_8), options);
    }

    public CompletableFuture<Void> write(String path, byte[] content, WriteOptions options) {
        final long requestId = withOperatorAndExecutor((op, exec) -> write(op, exec, path, content, options));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Metadata> stat(String path) {
        return stat(path, StatOptions.builder().build());
    }

    public CompletableFuture<Metadata> stat(String path, StatOptions options) {
        final long requestId = withOperatorAndExecutor((op, exec) -> stat(op, exec, path, options));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<byte[]> read(String path) {
        return read(path, ReadOptions.builder().build());
    }

    public CompletableFuture<byte[]> read(String path, long offset, long length) {
        return read(path, ReadOptions.builder().offset(offset).length(length).build());
    }

    public CompletableFuture<byte[]> read(String path, ReadOptions options) {
        final long requestId = withOperatorAndExecutor((op, exec) -> read(op, exec, path, options));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<PresignedRequest> presignRead(String path, Duration duration) {
        return presignRead(path, duration, ReadOptions.builder().build());
    }

    /**
     * Creates a presigned read request with the given options.
     *
     * <p>The service must support presigning this operation. Support for individual options
     * depends on the service. Send the returned HTTP method and headers along with the URI.
     *
     * @param path the path of the object.
     * @param duration how long the presigned request remains valid.
     * @param options the read options to include in the presigned request.
     * @return the presigned request.
     * @throws IllegalArgumentException if the duration is negative.
     * @throws ArithmeticException if the duration exceeds {@link Long#MAX_VALUE} nanoseconds.
     */
    public CompletableFuture<PresignedRequest> presignRead(String path, Duration duration, ReadOptions options) {
        final long expire = toNanos(duration);
        final long requestId = withOperatorAndExecutor((op, exec) -> presignRead(op, exec, path, expire, options));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<PresignedRequest> presignWrite(String path, Duration duration) {
        return presignWrite(path, duration, WriteOptions.builder().build());
    }

    /**
     * Creates a presigned write request with the given options.
     *
     * <p>The service must support presigning this operation. Support for individual options
     * depends on the service. Send the returned HTTP method and headers along with the URI.
     *
     * @param path the path of the object.
     * @param duration how long the presigned request remains valid.
     * @param options the write options to include in the presigned request.
     * @return the presigned request.
     * @throws IllegalArgumentException if the duration is negative.
     * @throws ArithmeticException if the duration exceeds {@link Long#MAX_VALUE} nanoseconds.
     */
    public CompletableFuture<PresignedRequest> presignWrite(String path, Duration duration, WriteOptions options) {
        final long expire = toNanos(duration);
        final long requestId = withOperatorAndExecutor((op, exec) -> presignWrite(op, exec, path, expire, options));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<PresignedRequest> presignStat(String path, Duration duration) {
        return presignStat(path, duration, StatOptions.builder().build());
    }

    /**
     * Creates a presigned stat request with the given options.
     *
     * <p>The service must support presigning this operation. Support for individual options
     * depends on the service. Send the returned HTTP method and headers along with the URI.
     *
     * @param path the path of the object.
     * @param duration how long the presigned request remains valid.
     * @param options the stat options to include in the presigned request.
     * @return the presigned request.
     * @throws IllegalArgumentException if the duration is negative.
     * @throws ArithmeticException if the duration exceeds {@link Long#MAX_VALUE} nanoseconds.
     */
    public CompletableFuture<PresignedRequest> presignStat(String path, Duration duration, StatOptions options) {
        final long expire = toNanos(duration);
        final long requestId = withOperatorAndExecutor((op, exec) -> presignStat(op, exec, path, expire, options));
        return AsyncRegistry.take(requestId);
    }

    /**
     * Converts a presign duration to the nanosecond count the native layer expects.
     *
     * <p>The native layer interprets the value as an unsigned 64-bit count, so reject
     * negative durations before passing the value to JNI.
     */
    private static long toNanos(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("duration must be non-negative");
        }
        return duration.toNanos();
    }

    public CompletableFuture<Void> delete(String path) {
        return delete(path, DeleteOptions.builder().build());
    }

    /**
     * Deletes the given path with the supplied options. A missing file is a successful
     * no-op when all conditions hold. For recursive directory deletion, include the
     * trailing slash in the path.
     *
     * @param path the path to delete
     * @param options the delete options
     * @return a future that completes exceptionally with an {@link OpenDALException}
     *     if an option is unsupported, a condition fails, or deletion fails
     */
    public CompletableFuture<Void> delete(String path, DeleteOptions options) {
        final long requestId = withOperatorAndExecutor((op, exec) -> delete(op, exec, path, options));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> createDir(String path) {
        final long requestId = withOperatorAndExecutor((op, exec) -> createDir(op, exec, path));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> copy(String sourcePath, String targetPath) {
        final long requestId = withOperatorAndExecutor((op, exec) -> copy(op, exec, sourcePath, targetPath));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> rename(String sourcePath, String targetPath) {
        final long requestId = withOperatorAndExecutor((op, exec) -> rename(op, exec, sourcePath, targetPath));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<Void> removeAll(String path) {
        final long requestId = withOperatorAndExecutor((op, exec) -> removeAll(op, exec, path));
        return AsyncRegistry.take(requestId);
    }

    public CompletableFuture<List<Entry>> list(String path) {
        return list(path, ListOptions.builder().build());
    }

    public CompletableFuture<List<Entry>> list(String path, ListOptions options) {
        final long requestId = withOperatorAndExecutor((op, exec) -> list(op, exec, path, options));
        final CompletableFuture<Entry[]> result = AsyncRegistry.take(requestId);
        return Objects.requireNonNull(result).thenApplyAsync(Arrays::asList);
    }

    private long withOperatorAndExecutor(LongBinaryOperator action) {
        return withHandle(op -> withExecutor(executor, exec -> action.applyAsLong(op, exec)));
    }

    private static long withExecutor(AsyncExecutor executor, LongUnaryOperator action) {
        return executor == null ? action.applyAsLong(0) : executor.withHandle(action::applyAsLong);
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long duplicate(long nativeHandle);

    private static native long constructor(String scheme, Map<String, String> map, long executorHandle);

    private static native long read(long nativeHandle, long executorHandle, String path, ReadOptions options);

    private static native long write(
            long nativeHandle, long executorHandle, String path, byte[] content, WriteOptions options);

    private static native long append(long nativeHandle, long executorHandle, String path, byte[] content);

    private static native long delete(long nativeHandle, long executorHandle, String path, DeleteOptions options);

    private static native long stat(long nativeHandle, long executorHandle, String path, StatOptions options);

    private static native long presignRead(
            long nativeHandle, long executorHandle, String path, long duration, ReadOptions options);

    private static native long presignWrite(
            long nativeHandle, long executorHandle, String path, long duration, WriteOptions options);

    private static native long presignStat(
            long nativeHandle, long executorHandle, String path, long duration, StatOptions options);

    private static native OperatorInfo makeOperatorInfo(long nativeHandle);

    private static native long makeBlockingOp(long nativeHandle, long executorHandle);

    private static native long createDir(long nativeHandle, long executorHandle, String path);

    private static native long copy(long nativeHandle, long executorHandle, String sourcePath, String targetPath);

    private static native long rename(long nativeHandle, long executorHandle, String sourcePath, String targetPath);

    private static native long removeAll(long nativeHandle, long executorHandle, String path);

    private static native long list(long nativeHandle, long executorHandle, String path, ListOptions options);
}
