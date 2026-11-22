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

import java.util.Objects;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Layer;
import org.apache.opendal.NativeObject;
import org.apache.opendal.OpenDALException;

/**
 * Routes storage operations by matching paths against glob patterns in the order the routes were added.
 * The first match wins. Unmatched paths use the operator to which the layer is applied.
 *
 * <p>Copy and rename select the service using the source path and operate within that service.
 * The layer reports capabilities based on the wrapped operator's capabilities, without aggregating
 * the targets' capabilities.
 *
 * <p>Close the layer after applying it to every operator that needs it. The resulting operators
 * retain the routes and can continue using them after the layer is closed.
 *
 * <pre>{@code
 * try (RouteLayer.RouteLayerBuilder builder = RouteLayer.builder()) {
 *     builder.route("archive/**", archive);
 *     try (RouteLayer layer = builder.build();
 *             AsyncOperator routed = primary.layer(layer)) {
 *         routed.write("archive/example.txt", "example").join();
 *     }
 * }
 * }</pre>
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.RouteLayer.html">RouteLayer's rustdoc</a>
 */
public final class RouteLayer extends Layer implements AutoCloseable {
    private long nativeHandle;

    private RouteLayer(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /**
     * Creates a builder with no routes. Close the builder if construction is abandoned before calling {@code build()}.
     *
     * @return a new route layer builder
     */
    public static RouteLayerBuilder builder() {
        return new RouteLayerBuilder();
    }

    @Override
    protected synchronized long layer(long nativeOp) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("RouteLayer is closed");
        }
        return doLayer(nativeOp, nativeHandle);
    }

    /** Releases the layer's reference to the routes. Operators already using the layer retain their routes. */
    @Override
    public synchronized void close() {
        if (nativeHandle != 0) {
            disposeLayer(nativeHandle);
            nativeHandle = 0;
        }
    }

    /** Collects ordered routes and builds one layer. */
    public static final class RouteLayerBuilder extends NativeObject {
        private RouteLayerBuilder() {
            super(createBuilder());
        }

        /**
         * Adds a route for paths matching the given glob. The target can be closed after this method returns.
         *
         * @param pattern the glob to match against paths relative to the operator root
         * @param target the operator to use for matching paths
         * @return this builder
         * @throws NullPointerException if the pattern or target is null
         * @throws IllegalStateException if the builder or target is closed
         */
        public synchronized RouteLayerBuilder route(String pattern, AsyncOperator target) {
            checkOpen();
            Objects.requireNonNull(pattern, "pattern");
            Objects.requireNonNull(target, "target");
            synchronized (target) {
                if (target.isDisposed()) {
                    throw new IllegalStateException("Route target is closed");
                }
                addRoute(nativeHandle, pattern, target);
            }
            return this;
        }

        /**
         * Compiles the route patterns and closes this builder, including when compilation fails.
         *
         * @return the route layer
         * @throws OpenDALException with code {@code ConfigInvalid} if a glob pattern is invalid
         * @throws IllegalStateException if the builder is closed
         */
        public synchronized RouteLayer build() {
            checkOpen();
            try {
                return new RouteLayer(buildLayer(nativeHandle));
            } finally {
                close();
            }
        }

        private void checkOpen() {
            if (isDisposed()) {
                throw new IllegalStateException("RouteLayerBuilder is closed");
            }
        }

        @Override
        public synchronized void close() {
            super.close();
        }

        @Override
        protected void disposeInternal(long handle) {
            disposeBuilder(handle);
        }
    }

    private static native long createBuilder();

    private static native void addRoute(long builder, String pattern, AsyncOperator target);

    private static native long buildLayer(long builder);

    private static native void disposeBuilder(long builder);

    private static native long doLayer(long nativeOp, long layer);

    private static native void disposeLayer(long layer);
}
