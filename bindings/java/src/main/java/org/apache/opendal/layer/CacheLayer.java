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
import java.util.Objects;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Layer;
import org.apache.opendal.NativeObject;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;

/**
 * Caches aligned chunks of reads in a separate operator. Writes and deletes through the layered
 * operator invalidate cached metadata for the affected paths.
 *
 * <p>Use a cache backend with capacity eviction to reclaim chunks from old object identities.
 * Use separate cache roots or instances for different tenants, endpoints, or credentials because
 * fresh cache hits can skip source I/O.
 *
 * <p>The layer retains its own reference to the cache operator. The supplied cache handle can be
 * closed after {@code build()} returns. Close the layer after applying it to every operator that
 * needs it. Those operators can still use the cache after the layer is closed.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.CacheLayer.html">CacheLayer's rustdoc</a>
 */
public final class CacheLayer extends Layer implements AutoCloseable {
    private long nativeHandle;

    private CacheLayer(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /**
     * Creates a builder using an asynchronous cache operator.
     *
     * @param cache the cache operator, which must support read, write, and delete
     * @return a new builder
     * @throws NullPointerException if the cache operator is null
     */
    public static CacheLayerBuilder builder(AsyncOperator cache) {
        return new CacheLayerBuilder(cache);
    }

    /**
     * Creates a builder using a blocking cache operator.
     *
     * @param cache the cache operator, which must support read, write, and delete
     * @return a new builder
     * @throws NullPointerException if the cache operator is null
     */
    public static CacheLayerBuilder builder(Operator cache) {
        return new CacheLayerBuilder(cache);
    }

    /** Configures the cache chunk size and metadata freshness. */
    public static final class CacheLayerBuilder {
        private final NativeObject cache;
        // Zero represents the default chunk size at the JNI boundary.
        private long chunkSize;
        private Duration ttl;

        private CacheLayerBuilder(NativeObject cache) {
            this.cache = Objects.requireNonNull(cache, "cache");
        }

        /**
         * Sets the aligned chunk size. The default is 1 MiB. The cache operator's maximum
         * write size caps the effective chunk size.
         *
         * @param chunkSize the positive chunk size in bytes
         * @return this builder
         * @throws IllegalArgumentException if the chunk size is zero or negative
         */
        public CacheLayerBuilder chunkSize(long chunkSize) {
            if (chunkSize <= 0) {
                throw new IllegalArgumentException("chunkSize must be positive");
            }
            this.chunkSize = chunkSize;
            return this;
        }

        /**
         * Sets the metadata freshness window. The default is 60 seconds. Zero revalidates
         * metadata before serving a reader. Reads of explicitly selected immutable versions
         * skip periodic revalidation.
         *
         * @param ttl the non-negative freshness window
         * @return this builder
         * @throws IllegalArgumentException if the duration is null or negative
         */
        public CacheLayerBuilder ttl(Duration ttl) {
            if (ttl == null || ttl.isNegative()) {
                throw new IllegalArgumentException("ttl must be non-negative");
            }
            this.ttl = ttl;
            return this;
        }

        /**
         * Builds a layer with its own reference to the cache operator.
         *
         * @return a new cache layer
         * @throws IllegalStateException if the cache operator is closed
         * @throws OpenDALException with code {@link OpenDALException.Code#ConfigInvalid} if the
         *     cache operator lacks read, write, or delete, or the chunk size exceeds the native size range
         */
        public CacheLayer build() {
            synchronized (cache) {
                if (cache.isDisposed()) {
                    throw new IllegalStateException("Cache operator is closed");
                }
                return new CacheLayer(createLayer(
                        cache,
                        cache instanceof Operator,
                        chunkSize,
                        ttl == null ? -1 : ttl.getSeconds(),
                        ttl == null ? 0 : ttl.getNano()));
            }
        }
    }

    @Override
    protected synchronized long layer(long nativeOp) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("CacheLayer is closed");
        }
        return doLayer(nativeOp, nativeHandle);
    }

    /** Releases this layer's reference to the cache. Already-layered operators retain theirs. */
    @Override
    public synchronized void close() {
        if (nativeHandle != 0) {
            disposeLayer(nativeHandle);
            nativeHandle = 0;
        }
    }

    private static native long createLayer(
            NativeObject cache, boolean blocking, long chunkSize, long ttlSeconds, int ttlNanos);

    private static native long doLayer(long nativeOp, long layer);

    private static native void disposeLayer(long layer);
}
