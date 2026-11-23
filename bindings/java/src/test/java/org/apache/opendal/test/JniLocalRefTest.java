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

package org.apache.opendal.test;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.HashMap;
import java.util.Map;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.WriteOptions;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for JNI local reference accumulation: listing a directory with many entries
 * must not pin one local reference per entry for the whole native call. Run the tests with
 * {@code -Xcheck:jni} to surface a local reference table overflow.
 */
public class JniLocalRefTest {
    private static final int ENTRY_COUNT = 250;
    private static final int USER_METADATA_COUNT = 5;

    private static WriteOptions writeOptionsWithUserMetadata() {
        final Map<String, String> userMetadata = new HashMap<>();
        for (int i = 0; i < USER_METADATA_COUNT; i++) {
            userMetadata.put("key-" + i, "value-" + i);
        }
        return WriteOptions.builder().userMetadata(userMetadata).build();
    }

    @Test
    void testBlockingListReleasesLocalReferences() {
        final WriteOptions options = writeOptionsWithUserMetadata();
        try (final Operator op = Operator.of(ServiceConfig.Memory.builder().build())) {
            for (int i = 0; i < ENTRY_COUNT; i++) {
                op.write("blocking/file-" + i, "x", options);
            }
            assertThat(op.list("blocking/")).hasSize(ENTRY_COUNT);
        }
    }

    @Test
    void testAsyncListReleasesLocalReferences() {
        final WriteOptions options = writeOptionsWithUserMetadata();
        try (final AsyncOperator op =
                AsyncOperator.of(ServiceConfig.Memory.builder().build())) {
            for (int i = 0; i < ENTRY_COUNT; i++) {
                op.write("async/file-" + i, "x", options).join();
            }
            assertThat(op.list("async/").join()).hasSize(ENTRY_COUNT);
        }
    }
}
