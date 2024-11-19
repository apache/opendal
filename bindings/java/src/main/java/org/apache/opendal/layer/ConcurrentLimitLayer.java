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
 * Users can control how many concurrent connections could be established between
 * OpenDAL and underlying storage services.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.ConcurrentLimitLayer.html">ConcurrentLimitLayer's rustdoc</a>
 */
public class ConcurrentLimitLayer extends Layer {
    private final long permits;

    /**
     * Create a new ConcurrentLimitLayer will specify permits.
     *
     * @param permits concurrent connections could be established
     */
    public ConcurrentLimitLayer(long permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive");
        }

        this.permits = permits;
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, permits);
    }

    private static native long doLayer(long nativeHandle, long permits);
}
