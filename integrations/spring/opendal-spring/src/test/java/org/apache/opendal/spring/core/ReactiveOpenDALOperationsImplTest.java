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

package org.apache.opendal.spring.core;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.OpenDALException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class ReactiveOpenDALOperationsImplTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void writeIsLazyAndReexecutesOnResubscription() {
        AtomicInteger serializeCalls = new AtomicInteger();
        OpenDALSerializer<String> serializer = countingStringSerializer(serializeCalls, new AtomicInteger());

        try (AsyncOperator operator = AsyncOperator.of("memory", Map.of())) {
            ReactiveOpenDALOperations<String> ops = new ReactiveOpenDALOperationsImpl<>(operator, serializer);

            Mono<Void> writeMono = ops.write("lazy-write", "initial");

            Assertions.assertEquals(0, serializeCalls.get());
            OpenDALException notFound = Assertions.assertThrows(
                OpenDALException.class,
                () -> ops.read("lazy-write").block(TIMEOUT));
            Assertions.assertEquals(OpenDALException.Code.NotFound, notFound.getCode());

            writeMono.block(TIMEOUT);
            Assertions.assertEquals(1, serializeCalls.get());
            Assertions.assertEquals("initial", ops.read("lazy-write").block(TIMEOUT));

            ops.write("lazy-write", "overwritten").block(TIMEOUT);
            Assertions.assertEquals("overwritten", ops.read("lazy-write").block(TIMEOUT));

            writeMono.block(TIMEOUT);
            Assertions.assertEquals(3, serializeCalls.get());
            Assertions.assertEquals("initial", ops.read("lazy-write").block(TIMEOUT));
        }
    }

    @Test
    void readIsLazyAndReexecutesOnResubscription() {
        AtomicInteger deserializeCalls = new AtomicInteger();
        OpenDALSerializer<String> serializer = countingStringSerializer(new AtomicInteger(), deserializeCalls);

        try (AsyncOperator operator = AsyncOperator.of("memory", Map.of())) {
            ReactiveOpenDALOperations<String> ops = new ReactiveOpenDALOperationsImpl<>(operator, serializer);

            Mono<String> readMono = ops.read("lazy-read");
            Assertions.assertEquals(0, deserializeCalls.get());

            ops.write("lazy-read", "v1").block(TIMEOUT);
            Assertions.assertEquals("v1", readMono.block(TIMEOUT));
            Assertions.assertEquals(1, deserializeCalls.get());

            ops.write("lazy-read", "v2").block(TIMEOUT);
            Assertions.assertEquals("v2", readMono.block(TIMEOUT));
            Assertions.assertEquals(2, deserializeCalls.get());
        }
    }

    @Test
    void deleteIsLazyAndReexecutesOnResubscription() {
        OpenDALSerializer<String> serializer = countingStringSerializer(new AtomicInteger(), new AtomicInteger());

        try (AsyncOperator operator = AsyncOperator.of("memory", Map.of())) {
            ReactiveOpenDALOperations<String> ops = new ReactiveOpenDALOperationsImpl<>(operator, serializer);

            ops.write("lazy-delete", "alive").block(TIMEOUT);
            Mono<Void> deleteMono = ops.delete("lazy-delete");

            Assertions.assertEquals("alive", ops.read("lazy-delete").block(TIMEOUT));

            deleteMono.block(TIMEOUT);
            OpenDALException firstDeleted = Assertions.assertThrows(
                OpenDALException.class,
                () -> ops.read("lazy-delete").block(TIMEOUT));
            Assertions.assertEquals(OpenDALException.Code.NotFound, firstDeleted.getCode());

            ops.write("lazy-delete", "recreated").block(TIMEOUT);
            deleteMono.block(TIMEOUT);
            OpenDALException secondDeleted = Assertions.assertThrows(
                OpenDALException.class,
                () -> ops.read("lazy-delete").block(TIMEOUT));
            Assertions.assertEquals(OpenDALException.Code.NotFound, secondDeleted.getCode());
        }
    }

    @Test
    void retryIssuesFreshOperation() {
        OpenDALSerializer<String> serializer = countingStringSerializer(new AtomicInteger(), new AtomicInteger());

        try (AsyncOperator operator = AsyncOperator.of("memory", Map.of())) {
            ReactiveOpenDALOperations<String> ops = new ReactiveOpenDALOperationsImpl<>(operator, serializer);

            String result = ops.read("retry-read")
                .retryWhen(Retry.max(1).doBeforeRetryAsync(
                    signal -> ops.write("retry-read", "recovered")))
                .block(TIMEOUT);

            Assertions.assertEquals("recovered", result);
        }
    }

    @Test
    void serializationErrorIsDeferredUntilSubscription() {
        OpenDALSerializer<String> failingSerializer = new OpenDALSerializer<>() {
            @Override
            public byte[] serialize(String value) {
                throw new IllegalStateException("serialize failed");
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };

        try (AsyncOperator operator = AsyncOperator.of("memory", Map.of())) {
            ReactiveOpenDALOperations<String> ops = new ReactiveOpenDALOperationsImpl<>(operator, failingSerializer);

            Mono<Void> writeMono = Assertions.assertDoesNotThrow(() -> ops.write("err-key", "value"));
            IllegalStateException error = Assertions.assertThrows(
                IllegalStateException.class,
                () -> writeMono.block(TIMEOUT));
            Assertions.assertEquals("serialize failed", error.getMessage());
        }
    }

    private static OpenDALSerializer<String> countingStringSerializer(
        AtomicInteger serializeCalls, AtomicInteger deserializeCalls) {
        return new OpenDALSerializer<>() {
            @Override
            public byte[] serialize(String value) {
                serializeCalls.incrementAndGet();
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(byte[] bytes) {
                deserializeCalls.incrementAndGet();
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
    }
}
