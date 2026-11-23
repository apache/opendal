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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Layer;
import org.apache.opendal.NativeObject;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.layer.MimeGuessLayer;
import org.junit.jupiter.api.Test;

class OperatorCloseTest {
    private static final ServiceConfig.Memory MEMORY =
            ServiceConfig.Memory.builder().build();
    private static final String PATH = "file.txt";
    private static final byte[] CONTENT = "OpenDAL".getBytes(StandardCharsets.UTF_8);

    @Test
    void asyncOperatorRejectsCallsAfterClose() {
        try (AsyncOperator op = AsyncOperator.of(MEMORY)) {
            op.close();
            assertThatIllegalStateException().isThrownBy(() -> op.layer(new MimeGuessLayer()));
            assertThatIllegalStateException().isThrownBy(op::duplicate);
            assertThatIllegalStateException().isThrownBy(op::blocking);
            assertThatIllegalStateException().isThrownBy(() -> op.read(PATH));
            assertThatIllegalStateException().isThrownBy(() -> op.write(PATH, CONTENT));
        }
    }

    @Test
    void blockingOperatorRejectsCallsAfterClose() {
        try (Operator op = Operator.of(MEMORY)) {
            op.close();
            assertThatIllegalStateException().isThrownBy(op::duplicate);
            assertThatIllegalStateException().isThrownBy(() -> op.read(PATH));
            assertThatIllegalStateException().isThrownBy(() -> op.write(PATH, CONTENT));
            assertThatIllegalStateException().isThrownBy(() -> op.createReader(PATH));
            assertThatIllegalStateException().isThrownBy(() -> op.createInputStream(PATH));
            assertThatIllegalStateException().isThrownBy(() -> op.createOutputStream(PATH));
        }
    }

    @Test
    void closedExecutorIsRejectedBeforeJni() {
        try (AsyncExecutor executor = AsyncExecutor.createTokioExecutor(1);
                AsyncOperator op = AsyncOperator.of(MEMORY, executor)) {
            executor.close();
            assertThatIllegalStateException().isThrownBy(() -> AsyncOperator.of(MEMORY, executor));
            assertThatIllegalStateException().isThrownBy(op::blocking);
            assertThatIllegalStateException().isThrownBy(() -> op.read(PATH));
        }
    }

    @Test
    void layerCanFinishAfterSourceCloses() throws Exception {
        final ExecutorService worker = Executors.newSingleThreadExecutor();
        final CountDownLatch applying = new CountDownLatch(1);
        final CountDownLatch closed = new CountDownLatch(1);
        final Layer layer = new MimeGuessLayer() {
            @Override
            protected long layer(long nativeOp) {
                applying.countDown();
                try {
                    assertThat(closed.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
                return super.layer(nativeOp);
            }
        };

        try (AsyncOperator source = AsyncOperator.of(MEMORY)) {
            source.write(PATH, CONTENT).join();
            final Future<AsyncOperator> result = worker.submit(() -> source.layer(layer));
            try {
                assertThat(applying.await(10, TimeUnit.SECONDS)).isTrue();
                source.close();
                assertThat(source.isDisposed()).isTrue();
                assertThatIllegalStateException().isThrownBy(source::duplicate);
            } finally {
                closed.countDown();
            }
            try (AsyncOperator layered = result.get(10, TimeUnit.SECONDS)) {
                assertThat(layered.read(PATH).join()).isEqualTo(CONTENT);
            }
        } finally {
            worker.shutdownNow();
        }
    }

    @Test
    void derivingOperatorsCanRaceWithClose() throws Exception {
        final Layer layer = new MimeGuessLayer();
        final ExecutorService workers = Executors.newFixedThreadPool(4);
        try (AsyncExecutor executor = AsyncExecutor.createTokioExecutor(1)) {
            for (int iteration = 0; iteration < 300; iteration++) {
                try (AsyncOperator source = AsyncOperator.of(MEMORY, executor)) {
                    source.write(PATH, CONTENT).join();
                    final CountDownLatch start = new CountDownLatch(1);
                    final Future<AsyncOperator> layering =
                            workers.submit(() -> derive(start, () -> source.layer(layer)));
                    final Future<AsyncOperator> duplicating = workers.submit(() -> derive(start, source::duplicate));
                    final Future<Operator> blocking = workers.submit(() -> derive(start, source::blocking));
                    final Future<?> closing = workers.submit(() -> {
                        start.await();
                        source.close();
                        return null;
                    });
                    start.countDown();

                    try (AsyncOperator layered = layering.get(10, TimeUnit.SECONDS);
                            AsyncOperator duplicate = duplicating.get(10, TimeUnit.SECONDS);
                            Operator synchronous = blocking.get(10, TimeUnit.SECONDS)) {
                        closing.get(10, TimeUnit.SECONDS);
                        if (layered != null) {
                            assertThat(layered.read(PATH).join()).isEqualTo(CONTENT);
                        }
                        if (duplicate != null) {
                            assertThat(duplicate.read(PATH).join()).isEqualTo(CONTENT);
                        }
                        if (synchronous != null) {
                            assertThat(synchronous.read(PATH)).isEqualTo(CONTENT);
                        }
                    }
                }
            }
        } finally {
            workers.shutdownNow();
        }
    }

    private static <T extends NativeObject> T derive(CountDownLatch start, Supplier<T> action)
            throws InterruptedException {
        start.await();
        try {
            return action.get();
        } catch (IllegalStateException ex) {
            assertThat(ex).hasMessage("AsyncOperator is closed");
            return null;
        }
    }
}
