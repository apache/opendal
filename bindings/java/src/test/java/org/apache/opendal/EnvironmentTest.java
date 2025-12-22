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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class EnvironmentTest {
    @Test
    public void testBuildClassifierLinuxGnuX8664() {
        assertThat(Environment.buildClassifier("linux", "x86_64", false)).isEqualTo("linux-x86_64");
    }

    @Test
    public void testBuildClassifierLinuxMuslX8664() {
        assertThat(Environment.buildClassifier("linux", "x86_64", true)).isEqualTo("linux-x86_64-musl");
    }

    @Test
    public void testBuildClassifierLinuxMuslAarch64() {
        assertThat(Environment.buildClassifier("linux", "aarch64", true)).isEqualTo("linux-aarch_64-musl");
    }

    @Test
    public void testBuildClassifierNonLinuxIgnoreMusl() {
        assertThat(Environment.buildClassifier("mac os x", "x86_64", true)).isEqualTo("osx-x86_64");
        assertThat(Environment.buildClassifier("windows", "x86_64", true)).isEqualTo("windows-x86_64");
    }

    @Test
    public void testIsMuslOverride() {
        final String key = "org.apache.opendal.libc";
        final String previous = System.getProperty(key);
        try {
            System.setProperty(key, "musl");
            assertThat(Environment.isMusl("x86_64")).isTrue();

            System.setProperty(key, "gnu");
            assertThat(Environment.isMusl("x86_64")).isFalse();
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }
}

