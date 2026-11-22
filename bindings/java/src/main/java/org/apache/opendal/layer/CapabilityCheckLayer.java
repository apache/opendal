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
 * Validates optional operation arguments against the capabilities of the wrapped service.
 * Rejects selected unsupported write, copy, and compose options with
 * {@link org.apache.opendal.OpenDALException.Code#Unsupported}, and drops the list limit hint
 * when the service does not support it.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.CapabilityCheckLayer.html">CapabilityCheckLayer's rustdoc</a>
 */
public class CapabilityCheckLayer extends Layer {

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp);
    }

    private static native long doLayer(long nativeHandle);
}
