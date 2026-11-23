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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.ListOptions;
import org.apache.opendal.NativeObject;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.layer.CacheLayer;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CacheLayerTest {
    private static final ServiceConfig.Memory MEMORY =
            ServiceConfig.Memory.builder().build();
    private static final String PATH = "object";

    @Test
    void readsPopulateTheCacheAndReuseChunksForRanges() {
        try (AsyncOperator source = AsyncOperator.of(MEMORY);
                AsyncOperator cache = AsyncOperator.of(MEMORY);
                CacheLayer layer = CacheLayer.builder(cache)
                        .chunkSize(4)
                        .ttl(Duration.ofSeconds(Long.MAX_VALUE))
                        .build();
                AsyncOperator cached = source.layer(layer)) {
            source.write(PATH, "abcdefghij").join();
            assertThat(cache.list("").join()).isEmpty();
            assertThat(cached.read(PATH).join())
                    .asString(StandardCharsets.UTF_8)
                    .isEqualTo("abcdefghij");
            assertThat(cache.list("", ListOptions.builder().recursive(true).build())
                            .join())
                    .filteredOn(entry -> entry.metadata.isFile())
                    .extracting(entry -> new String(cache.read(entry.path).join(), StandardCharsets.UTF_8))
                    .contains("abcd", "efgh", "ij");

            source.delete(PATH).join();
            assertThat(cached.read(PATH, 2, 7).join())
                    .asString(StandardCharsets.UTF_8)
                    .isEqualTo("cdefghi");
        }
    }

    @Test
    void writesAndDeletesInvalidateCachedReads() {
        try (AsyncOperator source = AsyncOperator.of(MEMORY);
                AsyncOperator cache = AsyncOperator.of(MEMORY);
                CacheLayer layer = CacheLayer.builder(cache).build();
                AsyncOperator cached = source.layer(layer)) {
            source.write(PATH, "original").join();
            assertThat(cached.read(PATH).join())
                    .asString(StandardCharsets.UTF_8)
                    .isEqualTo("original");

            cached.write(PATH, "modified").join();
            assertThat(source.read(PATH).join())
                    .asString(StandardCharsets.UTF_8)
                    .isEqualTo("modified");
            assertThat(cached.read(PATH).join())
                    .asString(StandardCharsets.UTF_8)
                    .isEqualTo("modified");

            cached.delete(PATH).join();
            assertThatThrownBy(() -> source.read(PATH).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
            assertThatThrownBy(() -> cached.read(PATH).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retainsTheCacheAfterClosingItsHandleAndTheLayer(boolean blocking) {
        try (AsyncOperator source = AsyncOperator.of(MEMORY)) {
            final AsyncOperator layered;
            try (NativeObject cache = blocking ? Operator.of(MEMORY) : AsyncOperator.of(MEMORY);
                    CacheLayer layer = (blocking
                                    ? CacheLayer.builder((Operator) cache)
                                    : CacheLayer.builder((AsyncOperator) cache))
                            .build()) {
                cache.close();
                layered = source.layer(layer);
            }
            try (AsyncOperator cached = layered) {
                source.write(PATH, "cached data").join();
                assertThat(cached.read(PATH).join())
                        .asString(StandardCharsets.UTF_8)
                        .isEqualTo("cached data");
                source.delete(PATH).join();
                assertThat(cached.read(PATH).join())
                        .asString(StandardCharsets.UTF_8)
                        .isEqualTo("cached data");
            }
        }
    }

    @Test
    void zeroTtlRevalidatesBeforeServingCachedReads() {
        try (AsyncOperator source = AsyncOperator.of(MEMORY);
                AsyncOperator cache = AsyncOperator.of(MEMORY);
                CacheLayer layer = CacheLayer.builder(cache).ttl(Duration.ZERO).build();
                AsyncOperator cached = source.layer(layer)) {
            source.write(PATH, "original").join();
            assertThat(cached.read(PATH).join())
                    .asString(StandardCharsets.UTF_8)
                    .isEqualTo("original");

            source.write(PATH, "modified").join();
            assertThat(cached.read(PATH).join())
                    .asString(StandardCharsets.UTF_8)
                    .isEqualTo("modified");
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void rejectsNonPositiveChunkSizes(long chunkSize) {
        try (AsyncOperator cache = AsyncOperator.of(MEMORY)) {
            assertThatThrownBy(() -> CacheLayer.builder(cache).chunkSize(chunkSize))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("chunkSize");
        }
    }

    @Test
    void rejectsNegativeTtl() {
        try (AsyncOperator cache = AsyncOperator.of(MEMORY)) {
            assertThatThrownBy(() -> CacheLayer.builder(cache).ttl(Duration.ofNanos(-1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ttl");
        }
    }

    @Test
    void rejectsAClosedCacheOperator() {
        final CacheLayer.CacheLayerBuilder builder;
        try (AsyncOperator cache = AsyncOperator.of(MEMORY)) {
            builder = CacheLayer.builder(cache);
        }
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void rejectsACacheOperatorWithoutWriteAndDelete() {
        try (AsyncOperator cache = AsyncOperator.of(
                ServiceConfig.Http.builder().endpoint("http://127.0.0.1:1").build())) {
            assertThatThrownBy(() -> CacheLayer.builder(cache).build())
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid));
        }
    }
}
