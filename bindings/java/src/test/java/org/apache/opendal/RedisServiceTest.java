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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import lombok.Cleanup;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
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
        assertThat(op.read("testAccessRedisService").join()).isEqualTo("Odin");
        op.delete("testAccessRedisService").join();
        op.stat("testAccessRedisService")
                .handle((r, e) -> {
                    assertThat(r).isNull();
                    assertThat(e).isInstanceOf(CompletionException.class).hasCauseInstanceOf(OpenDALException.class);
                    OpenDALException.Code code = ((OpenDALException) e.getCause()).getCode();
                    assertThat(code).isEqualTo(OpenDALException.Code.NotFound);
                    return null;
                })
                .join();
    }

    @Test
    public void testAccessRedisServiceBlocking() {
        assertThat(redisContainer.isRunning()).isTrue();

        final Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        params.put("endpoint", "tcp://127.0.0.1:" + redisContainer.getMappedPort(6379));
        @Cleanup final BlockingOperator op = new BlockingOperator("Redis", params);

        op.write("testAccessRedisServiceBlocking", "Odin");
        assertThat(op.read("testAccessRedisServiceBlocking")).isEqualTo("Odin");
        op.delete("testAccessRedisServiceBlocking");
        assertThatExceptionOfType(OpenDALException.class)
                .isThrownBy(() -> op.stat("testAccessRedisServiceBlocking"))
                .extracting(OpenDALException::getCode)
                .isEqualTo(OpenDALException.Code.NotFound);
    }
}
