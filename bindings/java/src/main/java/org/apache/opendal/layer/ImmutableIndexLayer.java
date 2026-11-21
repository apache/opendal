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
import org.apache.opendal.Layer;

/**
 * Provides an immutable in-memory listing index for services without native list support, such as HTTP.
 * Listings use the supplied keys. Non-recursive listings infer directories from key prefixes.
 */
public class ImmutableIndexLayer extends Layer {

    private final String[] keys;

    /**
     * Creates an index from a snapshot of the supplied keys. With no keys, listings are empty.
     *
     * @param keys paths relative to the operator root. Use a trailing slash to include a directory explicitly.
     * @throws NullPointerException if the array or any key is null
     */
    public ImmutableIndexLayer(String... keys) {
        this.keys = Objects.requireNonNull(keys, "keys").clone();
        for (String key : this.keys) {
            Objects.requireNonNull(key, "key");
        }
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, keys);
    }

    private static native long doLayer(long nativeOp, String[] keys);
}
