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
 * Detects duplicate entries during a listing.
 * The first duplicate causes the listing to fail with
 * {@link org.apache.opendal.OpenDALException.Code#Unexpected}.
 * A regular listing compares paths. A listing that includes versions or deleted entries compares
 * path and version pairs, treating a missing version ID as a distinct value.
 * The layer retains each entry's identity until the listing ends.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.SanityCheckLayer.html">SanityCheckLayer's rustdoc</a>
 */
public class SanityCheckLayer extends Layer {

    /** Creates a layer that checks listings for duplicates. */
    public SanityCheckLayer() {}

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp);
    }

    private static native long doLayer(long nativeHandle);
}
