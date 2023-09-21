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

import lombok.ToString;

@ToString
public class OperatorInfo {
    public final String scheme;
    public final String root;
    public final String name;
    public final Capability fullCapability;
    public final Capability nativeCapability;

    public OperatorInfo(
            String scheme, String root, String name, Capability fullCapability, Capability nativeCapability) {
        this.scheme = scheme;
        this.root = root;
        this.name = name;
        this.fullCapability = fullCapability;
        this.nativeCapability = nativeCapability;
    }
}
