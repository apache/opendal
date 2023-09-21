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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import org.apache.opendal.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@DisabledIfEnvironmentVariable(named = "NO_DOCKER", matches = "true")
public class RedisServiceTest {
    @Container
    private final GenericContainer<?> redisContainer = new GenericContainer<>("redis:7.2.1").withExposedPorts(6379);

    @Test
    public void testAccessRedisService() {
        assertThat(redisContainer.isRunning()).isTrue();

        final Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        params.put("endpoint", "tcp://127.0.0.1:" + redisContainer.getMappedPort(6379));
        @Cleanup final Operator op = new Operator("Redis", params);

        op.write("testAccessRedisService", "Odin").join();
        assertThat(op.read("testAccessRedisService").join()).isEqualTo("Odin".getBytes(StandardCharsets.UTF_8));
        op.delete("testAccessRedisService").join();
        assertThatThrownBy(() -> op.stat("testAccessRedisService").join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @Test
    public void testAccessRedisServiceBlocking() {
        assertThat(redisContainer.isRunning()).isTrue();

        final Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        params.put("endpoint", "tcp://127.0.0.1:" + redisContainer.getMappedPort(6379));
        @Cleanup final BlockingOperator op = new BlockingOperator("Redis", params);

        op.write("testAccessRedisServiceBlocking", "Odin");
        assertThat(op.read("testAccessRedisServiceBlocking")).isEqualTo("Odin".getBytes(StandardCharsets.UTF_8));
        op.delete("testAccessRedisServiceBlocking");
        assertThatExceptionOfType(OpenDALException.class)
                .isThrownBy(() -> op.stat("testAccessRedisServiceBlocking"))
                .extracting(OpenDALException::getCode)
                .isEqualTo(OpenDALException.Code.NotFound);
    }
}
