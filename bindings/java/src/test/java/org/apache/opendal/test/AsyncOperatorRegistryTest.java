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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.ListOptions;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.ReadOptions;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.StatOptions;
import org.apache.opendal.WriteOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class AsyncOperatorRegistryTest {
    private static final byte[] CONTENT = "content".getBytes(StandardCharsets.UTF_8);

    @Test
    @Timeout(30)
    void failedArgumentConversionDoesNotLeakRegistryEntries() throws Exception {
        try (AsyncOperator op = AsyncOperator.of(ServiceConfig.Memory.builder().build())) {
            awaitEmptyRegistry();

            assertThatThrownBy(() -> op.write("file", CONTENT, null)).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("write");

            assertThatThrownBy(() -> op.read("file", null)).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("read");

            assertThatThrownBy(() -> op.stat("file", null)).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("stat");

            assertThatThrownBy(() -> op.list("", null)).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("list");

            assertThatThrownBy(() -> op.delete("file", null)).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("delete");

            assertThatThrownBy(() -> op.createDir(null)).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("createDir");

            assertThatThrownBy(() -> op.copy(null, "target")).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("copy");

            assertThatThrownBy(() -> op.rename(null, "target")).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("rename");

            assertThatThrownBy(() -> op.removeAll(null)).isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("removeAll");

            final Duration duration = Duration.ofSeconds(1);
            assertThatThrownBy(() ->
                            op.presignRead(null, duration, ReadOptions.builder().build()))
                    .isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("presignRead");

            assertThatThrownBy(() -> op.presignWrite(
                            null, duration, WriteOptions.builder().build()))
                    .isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("presignWrite");

            assertThatThrownBy(() ->
                            op.presignStat(null, duration, StatOptions.builder().build()))
                    .isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("presignStat");

            assertThatThrownBy(() -> op.write(
                            "file",
                            CONTENT,
                            WriteOptions.builder().concurrent(0).build()))
                    .isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("write with an invalid option");

            assertThatThrownBy(() -> op.list("", ListOptions.builder().limit(0).build()))
                    .isInstanceOf(OpenDALException.class);
            assertRegistryEmpty("list with an invalid option");
        }
    }

    private static void assertRegistryEmpty(String operation) throws Exception {
        assertThat(registrySize())
                .as("AsyncRegistry entries after %s failed before starting", operation)
                .isZero();
    }

    private static void awaitEmptyRegistry() throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (registrySize() != 0) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(
                        "AsyncRegistry still holds " + registrySize() + " entries from earlier operations");
            }
            Thread.sleep(10);
        }
    }

    private static int registrySize() throws ReflectiveOperationException {
        // AsyncRegistry is private to AsyncOperator; reflection keeps the
        // production API unchanged while the test checks for leaked entries.
        final Class<?> registryClass = Class.forName("org.apache.opendal.AsyncOperator$AsyncRegistry");
        final Field registryField = registryClass.getDeclaredField("registry");
        registryField.setAccessible(true);
        final Map<?, ?> registry = (Map<?, ?>) registryField.get(registryClass.getEnumConstants()[0]);
        return registry.size();
    }
}
