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
import java.nio.charset.StandardCharsets;
import lombok.Cleanup;
import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.ServiceConfig;
import org.junit.jupiter.api.Test;

public class AsyncExecutorTest {
    @Test
    void testDedicatedTokioExecutor() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final int cores = Runtime.getRuntime().availableProcessors();
        @Cleanup final AsyncExecutor executor = AsyncExecutor.createTokioExecutor(cores);
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory, executor);
        assertThat(op.info).isNotNull();

        final String key = "key";
        final byte[] v0 = "v0".getBytes(StandardCharsets.UTF_8);
        final byte[] v1 = "v1".getBytes(StandardCharsets.UTF_8);
        op.write(key, v0).join();
        assertThat(op.read(key).join()).isEqualTo(v0);
        op.write(key, v1).join();
        assertThat(op.read(key).join()).isEqualTo(v1);

        assertThat(executor.isDisposed()).isFalse();
        executor.close();
        assertThat(executor.isDisposed()).isTrue();

        // @Cleanup will close executor once more, but we don't crash with the guard.
    }
}
