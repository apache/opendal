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

package org.apache.opendal.layer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.apache.opendal.Layer;

/**
 * Applies deadlines to control operations and individual I/O calls.
 * When a deadline expires, the layer cancels the in-flight call and reports an OpenDAL error.
 * Add this layer before {@link RetryLayer} to apply the deadline to each attempt.
 * Use {@link #builder()} to set timeouts for individual operations.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.TimeoutLayer.html">TimeoutLayer's rustdoc</a>
 */
public class TimeoutLayer extends Layer {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_IO_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration MAX_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);

    private final Duration timeout;
    private final Duration ioTimeout;
    private final EnumMap<Operation, Duration> operationTimeouts;

    /** Identifies an operation that can have its own timeout. */
    public enum Operation {
        /** Creates a directory. */
        CREATE_DIR("create_dir"),
        /** Reads a file. */
        READ("read"),
        /** Writes a file. */
        WRITE("write"),
        /** Copies a file. */
        COPY("copy"),
        /** Composes files. */
        COMPOSE("compose"),
        /** Renames a file. */
        RENAME("rename"),
        /** Restores a file. */
        RESTORE("restore"),
        /** Retrieves file or directory metadata. */
        STAT("stat"),
        /** Deletes files. */
        DELETE("delete"),
        /** Lists entries. */
        LIST("list"),
        /** Generates a presigned request. */
        PRESIGN("presign");

        private final String nativeName;

        Operation(String nativeName) {
            this.nativeName = nativeName;
        }
    }

    /**
     * Creates a layer with a 60-second timeout for control operations and a 10-second timeout for I/O calls.
     */
    public TimeoutLayer() {
        this(DEFAULT_TIMEOUT, DEFAULT_IO_TIMEOUT);
    }

    /**
     * Creates a layer with the supplied timeouts.
     *
     * @param timeout the timeout for control operations such as stat
     * @param ioTimeout the timeout for individual I/O calls such as reading or writing a buffer
     * @throws IllegalArgumentException if either duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    public TimeoutLayer(Duration timeout, Duration ioTimeout) {
        this(
                requireTimeout(timeout, "timeout"),
                requireTimeout(ioTimeout, "ioTimeout"),
                new EnumMap<>(Operation.class));
    }

    private TimeoutLayer(Duration timeout, Duration ioTimeout, EnumMap<Operation, Duration> operationTimeouts) {
        this.timeout = timeout;
        this.ioTimeout = ioTimeout;
        this.operationTimeouts = operationTimeouts;
    }

    private static Duration requireTimeout(Duration timeout, String name) {
        Objects.requireNonNull(timeout, name);
        if (timeout.isNegative() || timeout.compareTo(MAX_TIMEOUT) > 0) {
            throw new IllegalArgumentException(name + " must be between zero and Long.MAX_VALUE nanoseconds");
        }
        return timeout;
    }

    /**
     * Creates a builder with the default control operation and I/O timeouts.
     *
     * @return a new builder
     */
    public static TimeoutLayerBuilder builder() {
        return new TimeoutLayerBuilder();
    }

    /** Configures the default and per-operation timeouts for a timeout layer. */
    public static final class TimeoutLayerBuilder {
        private Duration timeout = DEFAULT_TIMEOUT;
        private Duration ioTimeout = DEFAULT_IO_TIMEOUT;
        private final EnumMap<Operation, Duration> operationTimeouts = new EnumMap<>(Operation.class);

        private TimeoutLayerBuilder() {}

        /**
         * Sets the default timeout for control operations such as stat.
         *
         * @param timeout the timeout for control operations
         * @return this builder
         * @throws NullPointerException if the timeout is null
         * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public TimeoutLayerBuilder timeout(Duration timeout) {
            this.timeout = requireTimeout(timeout, "timeout");
            return this;
        }

        /**
         * Sets the default timeout for individual I/O calls and concurrent I/O tasks.
         *
         * @param ioTimeout the timeout for individual I/O calls and tasks
         * @return this builder
         * @throws NullPointerException if the timeout is null
         * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public TimeoutLayerBuilder ioTimeout(Duration ioTimeout) {
            this.ioTimeout = requireTimeout(ioTimeout, "ioTimeout");
            return this;
        }

        /**
         * Overrides the default timeout for the specified operation. See
         * {@link TimeoutLayer#withOperationTimeout(Operation, Duration)} for timeout semantics.
         * Each call replaces the previous override for that operation.
         *
         * @param operation the operation whose timeout to override
         * @param timeout the timeout, which may be zero
         * @return this builder
         * @throws NullPointerException if either argument is null
         * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public TimeoutLayerBuilder operationTimeout(Operation operation, Duration timeout) {
            operationTimeouts.put(Objects.requireNonNull(operation, "operation"), requireTimeout(timeout, "timeout"));
            return this;
        }

        /**
         * Builds a timeout layer with the configured timeouts.
         *
         * @return a new timeout layer
         */
        public TimeoutLayer build() {
            return new TimeoutLayer(timeout, ioTimeout, new EnumMap<>(operationTimeouts));
        }
    }

    /**
     * Returns a new layer with the specified control operation timeout.
     *
     * @param timeout the timeout for control operations
     * @return a new layer with the same I/O timeout and per-operation overrides
     * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    public TimeoutLayer withTimeout(Duration timeout) {
        return new TimeoutLayer(requireTimeout(timeout, "timeout"), ioTimeout, operationTimeouts);
    }

    /**
     * Returns a new layer with the specified I/O timeout.
     *
     * @param ioTimeout the timeout for individual I/O calls
     * @return a new layer with the same control operation timeout and per-operation overrides
     * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    public TimeoutLayer withIoTimeout(Duration ioTimeout) {
        return new TimeoutLayer(timeout, requireTimeout(ioTimeout, "ioTimeout"), operationTimeouts);
    }

    /**
     * Returns a new layer with a timeout override for the specified operation.
     * The override replaces the control operation default for operations such as stat and rename.
     * For read, write, list, delete, copy, and compose, it replaces the I/O default for each
     * individual I/O call and concurrent I/O task. Each call replaces the previous override
     * for the same operation.
     *
     * @param operation the operation whose timeout to override
     * @param timeout the timeout, which may be zero
     * @return a new layer with the specified override
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    public TimeoutLayer withOperationTimeout(Operation operation, Duration timeout) {
        final EnumMap<Operation, Duration> overrides = new EnumMap<>(operationTimeouts);
        overrides.put(Objects.requireNonNull(operation, "operation"), requireTimeout(timeout, "timeout"));
        return new TimeoutLayer(this.timeout, ioTimeout, overrides);
    }

    @Override
    protected long layer(long nativeOp) {
        final String[] operations = new String[operationTimeouts.size()];
        final long[] timeouts = new long[operationTimeouts.size()];
        int index = 0;
        for (Map.Entry<Operation, Duration> entry : operationTimeouts.entrySet()) {
            operations[index] = entry.getKey().nativeName;
            timeouts[index] = entry.getValue().toNanos();
            index++;
        }
        return doLayerWithTimeouts(nativeOp, timeout.toNanos(), ioTimeout.toNanos(), operations, timeouts);
    }

    private static native long doLayerWithTimeouts(
            long nativeHandle,
            long timeoutNanos,
            long ioTimeoutNanos,
            String[] operations,
            long[] operationTimeoutNanos);
}
