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

package org.apache.opendal.behavior;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.opendal.BlockingOperator;
import org.apache.opendal.Capability;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractBehaviorTest {
    protected final String scheme;
    protected final Map<String, String> config;
    protected Operator operator;
    protected BlockingOperator blockingOperator;

    protected AbstractBehaviorTest(String scheme) {
        this(scheme, createSchemeConfig(scheme));
    }

    protected AbstractBehaviorTest(String scheme, Map<String, String> config) {
        this.scheme = scheme;
        this.config = config;
    }

    @BeforeAll
    public void setup() {
        assertThat(isSchemeEnabled(config))
                .describedAs("service test for " + scheme + " is not enabled.")
                .isTrue();
        this.operator = Operator.of(scheme, config);
        this.blockingOperator = BlockingOperator.of(scheme, config);
    }

    @AfterAll
    public void teardown() {
        if (operator != null) {
            operator.close();
            operator = null;
        }
        if (blockingOperator != null) {
            blockingOperator.close();
            blockingOperator = null;
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncWriteTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.read && capability.write);
        }

        /**
         * Read not exist file should return NotFound.
         */
        @Test
        public void testReadNotExist() {
            final String path = UUID.randomUUID().toString();
            assertThatThrownBy(() -> operator.read(path).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        /**
         * Read full content should match.
         */
        @Test
        public void testReadFull() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            operator.write(path, content).join();
            final byte[] actualContent = operator.read(path).join();
            assertThat(actualContent).isEqualTo(content);
            operator.delete(path).join();
        }

        /**
         * Stat not exist file should return NotFound.
         */
        @Test
        public void testStatNotExist() {
            final String path = UUID.randomUUID().toString();
            assertThatThrownBy(() -> operator.stat(path).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        /**
         * Stat existing file should return metadata.
         */
        @Test
        public void testStatFile() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            operator.write(path, content).join();
            try (final Metadata meta = operator.stat(path).join()) {
                assertThat(meta.isFile()).isTrue();
                assertThat(meta.getContentLength()).isEqualTo(content.length);
            }
            operator.delete(path).join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncAppendTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.writeCanAppend);
        }

        @Test
        public void testAppendCreateAppend() {
            final String path = UUID.randomUUID().toString();
            final byte[] contentOne = generateBytes();
            final byte[] contentTwo = generateBytes();

            operator.append(path, contentOne).join();
            operator.append(path, contentTwo).join();

            final byte[] actualContent = operator.read(path).join();
            assertThat(actualContent.length).isEqualTo(contentOne.length + contentTwo.length);
            assertThat(Arrays.copyOfRange(actualContent, 0, contentOne.length)).isEqualTo(contentOne);
            assertThat(Arrays.copyOfRange(actualContent, contentOne.length, actualContent.length))
                    .isEqualTo(contentTwo);

            operator.delete(path).join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class BlockingWriteTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = blockingOperator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.blocking);
        }

        /**
         * Read not exist file should return NotFound.
         */
        @Test
        public void testBlockingReadNotExist() {
            final String path = UUID.randomUUID().toString();
            assertThatThrownBy(() -> blockingOperator.read(path))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
        }

        /**
         * Read full content should match.
         */
        @Test
        public void testBlockingReadFull() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            blockingOperator.write(path, content);
            final byte[] actualContent = blockingOperator.read(path);
            assertThat(actualContent).isEqualTo(content);
            blockingOperator.delete(path);
        }

        /**
         * Stat existing file should return metadata.
         */
        @Test
        public void testBlockingStatFile() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            blockingOperator.write(path, content);
            try (final Metadata meta = blockingOperator.stat(path)) {
                assertThat(meta.isFile()).isTrue();
                assertThat(meta.getContentLength()).isEqualTo(content.length);
            }
            blockingOperator.delete(path);
        }
    }

    /**
     * Generates a byte array of random content.
     */
    public static byte[] generateBytes() {
        final Random random = new Random();
        final int size = random.nextInt(4 * 1024 * 1024) + 1;
        final byte[] content = new byte[size];
        random.nextBytes(content);
        return content;
    }

    protected static boolean isSchemeEnabled(Map<String, String> config) {
        final String turnOn = config.getOrDefault("test", "").toLowerCase();
        return turnOn.equals("on") || turnOn.equals("true");
    }

    protected static Map<String, String> createSchemeConfig(String scheme) {
        final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        final Map<String, String> config = new HashMap<>();
        final String prefix = "opendal_" + scheme.toLowerCase() + "_";
        for (DotenvEntry entry : dotenv.entries()) {
            final String key = entry.getKey().toLowerCase();
            if (key.startsWith(prefix)) {
                config.put(key.substring(prefix.length()), entry.getValue());
            }
        }
        return config;
    }
}
