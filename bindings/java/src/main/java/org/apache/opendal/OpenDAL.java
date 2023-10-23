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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.experimental.UtilityClass;

/**
 * Utility facade for top-level functions.
 */
@UtilityClass
public class OpenDAL {
    static {
        NativeLibrary.loadLibrary();
        final Set<String> enabledServices = new HashSet<>(Arrays.asList(loadEnabledServices()));
        ENABLED_SERVICES = Collections.unmodifiableSet(enabledServices);
    }

    private static final Set<String> ENABLED_SERVICES;

    public static Collection<String> enabledServices() {
        return ENABLED_SERVICES;
    }

    private static native String[] loadEnabledServices();
}
