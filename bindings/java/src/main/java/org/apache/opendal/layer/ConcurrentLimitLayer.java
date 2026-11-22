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

import org.apache.opendal.Layer;

/**
 * Limits concurrent storage operations. Operations without a dedicated limit share the default
 * permits. Reads, writes, and lists with dedicated limits use their own permits, so total storage
 * operation concurrency can exceed the default limit.
 *
 * <p>The optional HTTP request limit applies independently of the storage operation limits.
 * Each application of this layer creates its own set of permits.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.ConcurrentLimitLayer.html">ConcurrentLimitLayer's rustdoc</a>
 */
public class ConcurrentLimitLayer extends Layer {
    private final long permits;
    private final long readPermits;
    private final long writePermits;
    private final long listPermits;
    private final long httpPermits;

    /**
     * Creates a layer with one limit shared by all storage operations.
     *
     * @param permits the number of shared storage operation permits
     * @throws IllegalArgumentException if the permit count is not positive
     */
    public ConcurrentLimitLayer(long permits) {
        this(new ConcurrentLimitLayerBuilder(permits));
    }

    private ConcurrentLimitLayer(ConcurrentLimitLayerBuilder builder) {
        this.permits = builder.permits;
        this.readPermits = builder.readPermits;
        this.writePermits = builder.writePermits;
        this.listPermits = builder.listPermits;
        this.httpPermits = builder.httpPermits;
    }

    /**
     * Creates a builder for configuring dedicated storage operation and HTTP request limits.
     *
     * @param permits the number of permits shared by operations without a dedicated limit
     * @return a new builder
     * @throws IllegalArgumentException if the permit count is not positive
     */
    public static ConcurrentLimitLayerBuilder builder(long permits) {
        return new ConcurrentLimitLayerBuilder(permits);
    }

    /** Configures optional limits for reads, writes, lists, and HTTP requests. */
    public static final class ConcurrentLimitLayerBuilder {
        private final long permits;
        // Zero represents an unset limit at the JNI boundary.
        private long readPermits;
        private long writePermits;
        private long listPermits;
        private long httpPermits;

        private ConcurrentLimitLayerBuilder(long permits) {
            this.permits = requirePositive(permits, "permits");
        }

        /**
         * Sets a dedicated limit for reads. Reads use the shared limit by default.
         *
         * @param readPermits the number of read permits
         * @return this builder
         * @throws IllegalArgumentException if the permit count is not positive
         */
        public ConcurrentLimitLayerBuilder readPermits(long readPermits) {
            this.readPermits = requirePositive(readPermits, "readPermits");
            return this;
        }

        /**
         * Sets a dedicated limit for writes. Writes use the shared limit by default.
         *
         * @param writePermits the number of write permits
         * @return this builder
         * @throws IllegalArgumentException if the permit count is not positive
         */
        public ConcurrentLimitLayerBuilder writePermits(long writePermits) {
            this.writePermits = requirePositive(writePermits, "writePermits");
            return this;
        }

        /**
         * Sets a dedicated limit for lists. Lists use the shared limit by default.
         *
         * @param listPermits the number of list permits
         * @return this builder
         * @throws IllegalArgumentException if the permit count is not positive
         */
        public ConcurrentLimitLayerBuilder listPermits(long listPermits) {
            this.listPermits = requirePositive(listPermits, "listPermits");
            return this;
        }

        /**
         * Sets an independent limit for HTTP requests. By default this layer adds no HTTP limit.
         *
         * @param httpPermits the number of HTTP request permits
         * @return this builder
         * @throws IllegalArgumentException if the permit count is not positive
         */
        public ConcurrentLimitLayerBuilder httpPermits(long httpPermits) {
            this.httpPermits = requirePositive(httpPermits, "httpPermits");
            return this;
        }

        /**
         * Builds a concurrent limit layer with the configured limits.
         *
         * @return a new concurrent limit layer
         */
        public ConcurrentLimitLayer build() {
            return new ConcurrentLimitLayer(this);
        }

        private static long requirePositive(long permits, String name) {
            if (permits <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return permits;
        }
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayerWithLimits(nativeOp, permits, readPermits, writePermits, listPermits, httpPermits);
    }

    private static native long doLayerWithLimits(
            long nativeHandle, long permits, long readPermits, long writePermits, long listPermits, long httpPermits);
}
