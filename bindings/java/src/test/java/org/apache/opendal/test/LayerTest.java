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
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Entry;
import org.apache.opendal.Layer;
import org.apache.opendal.ListOptions;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.WriteOptions;
import org.apache.opendal.layer.CapabilityCheckLayer;
import org.apache.opendal.layer.CapabilityOverrideLayer;
import org.apache.opendal.layer.ChaosLayer;
import org.apache.opendal.layer.ConcurrentLimitLayer;
import org.apache.opendal.layer.HedgeLayer;
import org.apache.opendal.layer.ImmutableIndexLayer;
import org.apache.opendal.layer.MimeGuessLayer;
import org.apache.opendal.layer.RetryLayer;
import org.apache.opendal.layer.SanityCheckLayer;
import org.apache.opendal.layer.TailCutLayer;
import org.apache.opendal.layer.ThrottleLayer;
import org.apache.opendal.layer.TimeoutLayer;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class LayerTest {
    private static final ServiceConfig.Http OFFLINE_HTTP =
            ServiceConfig.Http.builder().endpoint("http://127.0.0.1:1").build();

    @Test
    void testOperatorWithRetryLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer retryLayer = RetryLayer.builder().build();
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(retryLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testOperatorWithConcurrentLimitLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer concurrentLimitLayer = new ConcurrentLimitLayer(1024);
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(concurrentLimitLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testConcurrentLimitLayerWithDedicatedPermits() throws Exception {
        final ConcurrentLimitLayer layer = ConcurrentLimitLayer.builder(1)
                .readPermits(2)
                .writePermits(3)
                .listPermits(4)
                .httpPermits(5)
                .build();
        @Cleanup
        final AsyncOperator op = AsyncOperator.of(ServiceConfig.Memory.builder().build());
        @Cleanup final AsyncOperator layeredOp = op.layer(layer);

        final byte[] content = {1, 2, 3};
        layeredOp.write("file.txt", content).get(5, TimeUnit.SECONDS);
        assertThat(layeredOp.read("file.txt").get(5, TimeUnit.SECONDS)).containsExactly(content);
        assertThat(layeredOp.list("").get(5, TimeUnit.SECONDS))
                .extracting(Entry::getPath)
                .containsExactly("file.txt");
        assertThat(layeredOp.stat("file.txt").get(5, TimeUnit.SECONDS).contentLength)
                .isEqualTo(content.length);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void testConcurrentLimitLayerRejectsNonPositivePermits(long invalid) {
        assertThatThrownBy(() -> new ConcurrentLimitLayer(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("permits ");
        assertThatThrownBy(() -> ConcurrentLimitLayer.builder(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("permits ");
        assertThatThrownBy(() -> ConcurrentLimitLayer.builder(1).readPermits(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("readPermits ");
        assertThatThrownBy(() -> ConcurrentLimitLayer.builder(1).writePermits(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("writePermits ");
        assertThatThrownBy(() -> ConcurrentLimitLayer.builder(1).listPermits(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("listPermits ");
        assertThatThrownBy(() -> ConcurrentLimitLayer.builder(1).httpPermits(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("httpPermits ");
    }

    @Test
    void testCapabilityCheckLayerRejectsUnsupportedWriteContentType() {
        @Cleanup
        final AsyncOperator op = AsyncOperator.of(ServiceConfig.Memory.builder().build());
        @Cleanup
        final AsyncOperator overriddenOp = op.layer(new CapabilityOverrideLayer("write_with_content_type=false"));
        @Cleanup final AsyncOperator layeredOp = overriddenOp.layer(new CapabilityCheckLayer());

        final WriteOptions options =
                WriteOptions.builder().contentType("text/plain").build();
        assertThatThrownBy(() -> layeredOp.write("file.txt", "hello", options).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unsupported))
                .hasMessageContaining("content_type");

        layeredOp.write("file.txt", "hello").join();
        assertThat(op.stat("file.txt").join().contentLength).isEqualTo(5);
    }

    @Test
    void testChaosLayerReadErrorRatioBoundaries() {
        @Cleanup
        final AsyncOperator op = AsyncOperator.of(ServiceConfig.Memory.builder().build());
        @Cleanup final AsyncOperator noErrors = op.layer(new ChaosLayer(0.0));
        @Cleanup final AsyncOperator allErrors = op.layer(new ChaosLayer(1.0));

        final byte[] content = {1, 2, 3};
        op.write("file.txt", content).join();
        assertThat(noErrors.read("file.txt").join()).containsExactly(content);
        assertThatThrownBy(() -> allErrors.read("file.txt").join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unexpected))
                .hasMessageContaining("I am your chaos!");
    }

    @ParameterizedTest
    @ValueSource(doubles = {-0.1, 1.1, Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY})
    void testChaosLayerRejectsInvalidErrorRatio(double invalid) {
        assertThatThrownBy(() -> new ChaosLayer(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("errorRatio ");
    }

    @Test
    void testSanityCheckLayerListsMemoryEntries() {
        @Cleanup
        final AsyncOperator op = AsyncOperator.of(ServiceConfig.Memory.builder().build());
        @Cleanup final AsyncOperator layeredOp = op.layer(new SanityCheckLayer());

        op.write("first.txt", "first").join();
        op.write("second.txt", "second").join();
        assertThat(layeredOp.list("").join())
                .extracting(Entry::getPath)
                .containsExactlyInAnyOrder("first.txt", "second.txt");
    }

    @Test
    void testHedgeLayerCompletesStatWhileFirstRequestIsStalled() throws Exception {
        final CountDownLatch releaseFirstResponse = new CountDownLatch(1);
        final AtomicInteger requests = new AtomicInteger();
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        final ExecutorService handlers = Executors.newFixedThreadPool(2);
        try {
            server.setExecutor(handlers);
            server.createContext("/file", exchange -> {
                try {
                    if (requests.incrementAndGet() == 1) {
                        releaseFirstResponse.await();
                        return;
                    }
                    exchange.getResponseHeaders().set("Content-Length", "7");
                    exchange.sendResponseHeaders(200, -1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.start();

            final ServiceConfig.Http config = ServiceConfig.Http.builder()
                    .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                    .build();
            final HedgeLayer hedge = HedgeLayer.builder()
                    .delay(Duration.ofNanos(Long.MAX_VALUE))
                    .statDelay(Duration.ofMillis(25))
                    .build();
            @Cleanup final AsyncOperator op = AsyncOperator.of(config);
            @Cleanup final AsyncOperator layeredOp = op.layer(hedge);

            assertThat(layeredOp.stat("file").get(5, TimeUnit.SECONDS).contentLength)
                    .isEqualTo(7);
            assertThat(requests.get()).isEqualTo(2);
        } finally {
            releaseFirstResponse.countDown();
            server.stop(0);
            handlers.shutdownNow();
        }
    }

    @Test
    void testHedgeLayerRejectsInvalidDurations() {
        for (Duration invalid : Arrays.asList(
                null, Duration.ofNanos(-1), Duration.ofNanos(Long.MAX_VALUE).plusNanos(1))) {
            assertThatThrownBy(() -> HedgeLayer.builder().delay(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("delay ");
            assertThatThrownBy(() -> HedgeLayer.builder().statDelay(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("statDelay ");
            assertThatThrownBy(() -> HedgeLayer.builder().readDelay(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("readDelay ");
        }
    }

    @Test
    void testHedgeLayerRejectsNegativeMaxHedges() {
        assertThatThrownBy(() -> HedgeLayer.builder().maxHedges(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("maxHedges ");
    }

    @Test
    void testTailCutLayerCancelsSlowTailRequest() throws Exception {
        final CountDownLatch releaseResponse = new CountDownLatch(1);
        final AtomicInteger requests = new AtomicInteger();
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try {
            server.createContext("/file", exchange -> {
                try {
                    if (requests.incrementAndGet() == 6) {
                        releaseResponse.await();
                        return;
                    }
                    // Keep the HTTP client from reusing warm-up connections on JDK 11.
                    exchange.getResponseHeaders().set("Connection", "close");
                    exchange.getResponseHeaders().set("Content-Length", "7");
                    exchange.sendResponseHeaders(200, -1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.start();

            final ServiceConfig.Http config = ServiceConfig.Http.builder()
                    .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                    .build();
            final TailCutLayer tailCut = TailCutLayer.builder()
                    .percentile(95)
                    .safetyFactor(1.0)
                    .minSamples(5)
                    .minDeadline(Duration.ofMillis(50))
                    .maxDeadline(Duration.ofMillis(200))
                    .build();
            @Cleanup final AsyncOperator op = AsyncOperator.of(config);
            @Cleanup final AsyncOperator layeredOp = op.layer(tailCut);

            for (int i = 0; i < 5; i++) {
                assertThat(layeredOp.stat("file").get(5, TimeUnit.SECONDS).contentLength)
                        .isEqualTo(7);
            }
            assertThatThrownBy(() -> layeredOp.stat("file").get(5, TimeUnit.SECONDS))
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unexpected))
                    .hasMessageContaining("cancelled by tail cut");
        } finally {
            releaseResponse.countDown();
            server.stop(0);
        }
    }

    @Test
    void testTailCutLayerRejectsInvalidArguments() {
        for (int invalid : new int[] {49, 100}) {
            assertThatThrownBy(() -> TailCutLayer.builder().percentile(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("percentile ");
        }
        for (double invalid : new double[] {0.9, 5.1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThatThrownBy(() -> TailCutLayer.builder().safetyFactor(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("safetyFactor ");
        }
        final Duration overflowing = Duration.ofNanos(Long.MAX_VALUE).plusNanos(1);
        for (Duration invalid : Arrays.asList(
                null, Duration.ofNanos(-1), Duration.ofSeconds(120).plusNanos(1), overflowing)) {
            assertThatThrownBy(() -> TailCutLayer.builder().window(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("window ");
        }
        assertThatThrownBy(() -> TailCutLayer.builder().minSamples(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("minSamples ");
        for (Duration invalid : Arrays.asList(null, Duration.ofNanos(-1), overflowing)) {
            assertThatThrownBy(() -> TailCutLayer.builder().minDeadline(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("minDeadline ");
            assertThatThrownBy(() -> TailCutLayer.builder().maxDeadline(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("maxDeadline ");
        }
        assertThatThrownBy(() -> TailCutLayer.builder()
                        .minDeadline(Duration.ofSeconds(2))
                        .maxDeadline(Duration.ofSeconds(1))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minDeadline must be less than or equal to maxDeadline");
    }

    @Test
    void testImmutableIndexLayerListsHttpKeys() {
        final String[] keys = {"readme.txt", "docs/guide.txt", "docs/examples/sample.txt", "empty/"};
        final Layer index = new ImmutableIndexLayer(keys);
        keys[0] = "changed.txt";

        @Cleanup final AsyncOperator op = AsyncOperator.of(OFFLINE_HTTP);
        assertThat(op.info.capability.list).isFalse();
        @Cleanup final AsyncOperator layeredOp = op.layer(index);
        op.close();

        assertThat(layeredOp.info.capability.list).isTrue();
        assertThat(layeredOp.info.capability.listWithRecursive).isTrue();
        assertThat(layeredOp.list("").join())
                .extracting(Entry::getPath)
                .containsExactlyInAnyOrder("readme.txt", "docs/", "empty/");
        assertThat(layeredOp.list("docs/").join())
                .extracting(Entry::getPath)
                .containsExactlyInAnyOrder("docs/guide.txt", "docs/examples/");
        assertThat(layeredOp
                        .list("", ListOptions.builder().recursive(true).build())
                        .join())
                .extracting(Entry::getPath)
                .containsExactlyInAnyOrder("readme.txt", "docs/guide.txt", "docs/examples/sample.txt", "empty/");
    }

    @Test
    void testImmutableIndexLayerWithNoKeys() {
        @Cleanup final AsyncOperator op = AsyncOperator.of(OFFLINE_HTTP);
        try (AsyncOperator layeredOp = op.layer(new ImmutableIndexLayer())) {
            assertThat(layeredOp.list("").join()).isEmpty();
        }
        assertThatThrownBy(() -> op.list("").join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unsupported));
    }

    @Test
    void testImmutableIndexLayerRejectsNullKeys() {
        assertThatThrownBy(() -> new ImmutableIndexLayer((String[]) null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ImmutableIndexLayer("key", null)).isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @CsvSource({"txt, text/plain", "json, application/json"})
    void testMimeGuessLayer(String extension, String contentType) {
        @Cleanup
        final AsyncOperator op = AsyncOperator.of(ServiceConfig.Memory.builder().build());
        @Cleanup final AsyncOperator layeredOp = op.layer(new MimeGuessLayer());

        final String path = "file." + extension;
        op.write(path, "hello").join();
        assertThat(op.stat(path).join().contentType).isNull();
        assertThat(layeredOp.stat(path).join().contentType).isEqualTo(contentType);

        final String layeredPath = "layered." + extension;
        layeredOp.write(layeredPath, "hello").join();
        assertThat(op.stat(layeredPath).join().contentType).isEqualTo(contentType);
    }

    @Test
    void testTimeoutLayer() throws Exception {
        final CountDownLatch releaseResponse = new CountDownLatch(1);
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/file", exchange -> {
            try {
                releaseResponse.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            final ServiceConfig.Http config = ServiceConfig.Http.builder()
                    .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                    .build();
            @Cleanup final AsyncOperator op = AsyncOperator.of(config);
            @Cleanup final AsyncOperator layeredOp = op.layer(new TimeoutLayer().withTimeout(Duration.ofMillis(100)));
            assertThatThrownBy(() -> layeredOp.stat("file").get(5, TimeUnit.SECONDS))
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unexpected))
                    .hasMessageContaining("operation timeout reached");
        } finally {
            releaseResponse.countDown();
            server.stop(0);
        }
    }

    @Test
    void testTimeoutLayerRejectsInvalidDurations() {
        for (Duration invalid : Arrays.asList(
                Duration.ofNanos(-1), Duration.ofNanos(Long.MAX_VALUE).plusNanos(1))) {
            assertThatThrownBy(() -> new TimeoutLayer().withTimeout(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("timeout ");
            assertThatThrownBy(() -> new TimeoutLayer().withIoTimeout(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("ioTimeout ");
        }
    }

    @Test
    void testThrottleLayerEnforcesBurst() {
        @Cleanup
        final AsyncOperator op = AsyncOperator.of(ServiceConfig.Memory.builder().build());
        @Cleanup final AsyncOperator layeredOp = op.layer(new ThrottleLayer(0xFFFFFFFFL, 2));

        assertThatThrownBy(() -> layeredOp.write("too-large.txt", "abc").join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.RateLimited));
        layeredOp.write("within-burst.txt", "ab").join();
        assertThat(op.stat("within-burst.txt").join().contentLength).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 0x100000000L})
    void testThrottleLayerRejectsOutOfRangeArguments(long invalid) {
        assertThatThrownBy(() -> new ThrottleLayer(invalid, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("bandwidth ");
        assertThatThrownBy(() -> new ThrottleLayer(1, invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("burst ");
    }
}
