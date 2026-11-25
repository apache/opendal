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
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorInputStream;
import org.apache.opendal.OperatorOutputStream;
import org.apache.opendal.OperatorReader;
import org.apache.opendal.ReadOptions;
import org.apache.opendal.ReaderOptions;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.WriteOptions;
import org.apache.opendal.layer.ThrottleLayer;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class OperatorInputOutputStreamTest {
    @TempDir
    private static Path tempDir;

    @Test
    void testReadWriteWithStream() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();

        try (final Operator op = Operator.of(fs)) {
            final String path = "OperatorInputOutputStreamTest.txt";
            final long multi = 1024 * 1024;

            try (final OperatorOutputStream os = op.createOutputStream(path)) {
                for (long i = 0; i < multi; i++) {
                    os.write("[content] OperatorInputStreamTest\n".getBytes());
                }
            }

            try (final OperatorInputStream is = op.createInputStream(path)) {
                final Stream<String> lines = new BufferedReader(new InputStreamReader(is)).lines();
                final AtomicLong count = new AtomicLong();
                lines.forEach((line) -> {
                    assertThat(line).isEqualTo("[content] OperatorInputStreamTest");
                    count.incrementAndGet();
                });
                assertThat(count.get()).isEqualTo(multi);
            }
        }
    }

    @Test
    void testCreateInputStreamWithOptions() {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            final String path = "testCreateInputStreamWithOptions.txt";
            final String content = "0123456789";
            op.write(path, content);

            try (final OperatorInputStream is = op.createInputStream(
                    path, ReadOptions.builder().offset(4L).length(5L).build())) {
                final byte[] buffer = new byte[5];
                final int read = is.read(buffer, 0, 5);
                assertThat(read).isEqualTo(5);
                assertThat(new String(buffer)).isEqualTo("45678");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void testChunkedInputStream() throws Exception {
        final byte[] content = new byte[4 * 1024 + 13];
        new Random(8252).nextBytes(content);
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            final String path = "chunked.bin";
            op.write(path, content);
            final ReaderOptions options = ReaderOptions.builder()
                    .concurrent(4)
                    .chunk(1024)
                    .prefetch(2)
                    .build();
            try (final OperatorReader reader = op.createReader(path, options);
                    final OperatorInputStream in = reader.createInputStream()) {
                assertThat(IOUtils.toByteArray(in)).isEqualTo(content);
                assertThat(in.read()).isEqualTo(-1);
            }
        }
    }

    @Test
    void testEmptyInputStreamWithReaderOptions() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            final String path = "empty.bin";
            op.write(path, new byte[0]);
            final ReaderOptions options =
                    ReaderOptions.builder().chunk(2).contentLengthHint(0).build();
            try (final OperatorReader reader = op.createReader(path, options);
                    final OperatorInputStream in = reader.createInputStream()) {
                assertThat(in.read()).isEqualTo(-1);
            }
        }
    }

    static Stream<Arguments> invalidReaderOptions() {
        return Stream.of(
                Arguments.of(ReaderOptions.builder().concurrent(0).build(), "concurrent"),
                Arguments.of(ReaderOptions.builder().concurrent(-1).build(), "concurrent"),
                Arguments.of(ReaderOptions.builder().chunk(0).build(), "chunk"),
                Arguments.of(ReaderOptions.builder().chunk(-2).build(), "chunk"),
                Arguments.of(ReaderOptions.builder().prefetch(-1).build(), "prefetch"),
                Arguments.of(ReaderOptions.builder().contentLengthHint(-2).build(), "contentLengthHint"));
    }

    @ParameterizedTest
    @MethodSource("invalidReaderOptions")
    void testInvalidReaderOptions(ReaderOptions options, String field) {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            assertThatThrownBy(() -> op.createReader("invalid-options", options))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid))
                    .hasMessageContaining(field);
        }
    }

    @Test
    void testBulkWriteAvoidsScalarDispatch() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            final int[] scalarCalls = {0};
            final byte[] content = new byte[3 * 16384 + 17];
            new Random(8244).nextBytes(content);
            try (final OperatorOutputStream out =
                    new OperatorOutputStream(
                            op, "bulk-dispatch", WriteOptions.builder().build()) {
                        @Override
                        public void write(int b) throws java.io.IOException {
                            scalarCalls[0]++;
                            super.write(b);
                        }
                    }) {
                out.write(content);
            }
            assertThat(op.read("bulk-dispatch")).isEqualTo(content);
            assertThat(scalarCalls[0]).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 16384})
    void testMixedBulkWrites(int bufferSize) throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        final byte[] content = new byte[3 * bufferSize + 13];
        new Random(8244).nextBytes(content);
        final ByteArrayOutputStream expected = new ByteArrayOutputStream();
        try (final Operator op = Operator.of(fs)) {
            try (final OperatorOutputStream out = new OperatorOutputStream(
                    op, "mixed", bufferSize, WriteOptions.builder().build())) {
                out.write(255);
                expected.write(255);
                out.write(content, 2, bufferSize - 1);
                expected.write(content, 2, bufferSize - 1);
                out.write(content, 1, content.length - 2);
                expected.write(content, 1, content.length - 2);
                out.flush();
                out.write(content, 0, 3);
                expected.write(content, 0, 3);
                Arrays.fill(content, (byte) 0);
                out.write(127);
                expected.write(127);
            }
            assertThat(op.read("mixed")).isEqualTo(expected.toByteArray());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {16383, 16384, 16385, 1048575, 1048576, 1048577})
    void testBulkWriteBoundariesWithReusedSlice(int length) throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        final WriteOptions options =
                WriteOptions.builder().chunk(4 * 1024 * 1024).build();
        final byte[] source = new byte[length + 17];
        final byte[] small = {3, 5, 7};
        final Random random = new Random(8244);
        final ByteArrayOutputStream expected = new ByteArrayOutputStream();
        final String path = "bulk-slice-" + length;
        try (final Operator op = Operator.of(fs)) {
            try (final OperatorOutputStream out = op.createOutputStream(path, options)) {
                out.write(255);
                expected.write(255);
                out.write(small);
                expected.write(small);

                random.nextBytes(source);
                out.write(source, 5, length);
                expected.write(source, 5, length);
                out.flush();
                random.nextBytes(source);
                out.write(source, 11, length);
                expected.write(source, 11, length);
                Arrays.fill(source, (byte) 0);

                out.flush();
                out.flush();
                out.write(small);
                expected.write(small);
                out.write(127);
                expected.write(127);
            }
            assertThat(op.read(path)).isEqualTo(expected.toByteArray());
        }
    }

    @Test
    void testOutputStreamAfterClose() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        final String path = "closed-output";
        try (final Operator op = Operator.of(fs);
                final OperatorOutputStream out = op.createOutputStream(path)) {
            out.write(42);
            out.close();
            out.close();

            assertThatThrownBy(() -> out.write(1)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> out.write(new byte[16384], 0, 16384)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> out.write(new byte[0])).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(out::flush).isInstanceOf(IllegalStateException.class);
            assertThat(op.read(path)).isEqualTo(new byte[] {42});
        }
    }

    @Test
    void testOutputStreamCloseReleasesWriterWhenFlushFails() throws Exception {
        try (final AsyncOperator op =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                final AsyncOperator throttled = op.layer(new ThrottleLayer(1024, 4));
                final Operator layeredOp = throttled.blocking()) {
            final String path = "close-flush-failure";
            final OperatorOutputStream out = layeredOp.createOutputStream(path);
            // The 8 buffered bytes exceed the throttle burst of 4, so the flush
            // performed by close() fails with RateLimited.
            out.write(new byte[8]);

            assertThatThrownBy(out::close)
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited))
                    // close() is the first caller to observe the failed flush, so the
                    // original exception is rethrown instead of being wrapped.
                    .hasNoCause();

            // A failed close must not publish the object, not even an empty one.
            assertNotExist(layeredOp, path);

            // The native writer is released even though flush() failed, so the
            // stream rejects any further use instead of leaking the handle.
            assertThatThrownBy(() -> out.write(1)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(out::flush).isInstanceOf(IllegalStateException.class);

            // Closing again is a safe no-op that does not touch the released writer.
            out.close();
            out.close();
        }
    }

    @Test
    void testOutputStreamCloseDiscardsPartialWrite() throws Exception {
        try (final AsyncOperator op =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                final AsyncOperator throttled = op.layer(new ThrottleLayer(1024, 4));
                final Operator layeredOp = throttled.blocking()) {
            final String path = "partial-flush-failure";
            final OperatorOutputStream out = layeredOp.createOutputStream(path);
            // The first chunk fits into the burst and reaches the native writer.
            out.write(new byte[4]);
            out.flush();
            assertNotExist(layeredOp, path);

            // The second chunk exceeds the burst, so this flush fails after the
            // native writer already holds the first chunk.
            out.write(new byte[8]);
            assertThatThrownBy(out::flush).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited));

            // close() reports the unrecovered failure and discards the writer.
            // Committing would publish a truncated object.
            assertThatThrownBy(out::close).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited));
            assertNotExist(layeredOp, path);

            assertThatThrownBy(() -> out.write(1)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(out::flush).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testOutputStreamTryWithResourcesKeepsOriginalFailure() throws Exception {
        try (final AsyncOperator op =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                final AsyncOperator throttled = op.layer(new ThrottleLayer(1024, 4));
                final Operator layeredOp = throttled.blocking()) {
            final String path = "try-with-resources-failure";
            final Throwable[] thrown = new Throwable[1];
            try (final OperatorOutputStream out = layeredOp.createOutputStream(path)) {
                out.write(new byte[4]);
                out.flush();
                out.write(new byte[8]);
                out.flush();
            } catch (Throwable t) {
                thrown[0] = t;
            }

            // close() must not add the original failure as a suppressed exception of itself,
            // because the JVM rejects self-suppression with IllegalArgumentException.
            assertThat(thrown[0]).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited));
            assertNotExist(layeredOp, path);
        }
    }

    @Test
    void testOutputStreamCloseDiscardsWhenFlushOverrideFails() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        final String path = "override-flush-failure";
        final RuntimeException flushFailure = new RuntimeException("flush failed");
        try (final Operator op = Operator.of(fs)) {
            final OperatorOutputStream out =
                    new OperatorOutputStream(op, path, WriteOptions.builder().build()) {
                        @Override
                        public synchronized void flush() {
                            // A subclass can fail without reaching the native writer, so the
                            // stream never records this failure in its own field.
                            throw flushFailure;
                        }
                    };
            out.write(new byte[8]);

            // Committing here would publish the 8 buffered bytes that never reached the service.
            assertThatThrownBy(out::close)
                    .as("first failure observed by close()")
                    .isSameAs(flushFailure);
            assertNotExist(op, path);

            // The native writer is released even though the stream never flushed.
            assertThatThrownBy(() -> out.write(1)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testOutputStreamTryWithResourcesKeepsOriginalError() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        final String path = "try-with-resources-error";
        final Error flushFailure = new Error("flush failed");
        final int[] flushes = {0};
        final Throwable[] thrown = new Throwable[1];
        try (final Operator op = Operator.of(fs)) {
            try (final OperatorOutputStream out =
                    new OperatorOutputStream(op, path, 4, WriteOptions.builder().build()) {
                        @Override
                        public synchronized void flush() {
                            flushes[0]++;
                            throw flushFailure;
                        }
                    }) {
                out.write(new byte[] {1, 2});
                // Filling the buffer flushes it, which fails.
                out.write(new byte[] {3, 4});
            } catch (Throwable t) {
                thrown[0] = t;
            }

            // close() must not rethrow the Error the body is already propagating, because
            // try-with-resources would suppress it into itself and the JVM would replace the
            // failure with IllegalArgumentException: Self-suppression not permitted.
            assertThat(thrown[0]).as("original Error propagates").isSameAs(flushFailure);
            // The failure was recorded when write() ascended through flush(), so close()
            // discards without flushing again.
            assertThat(flushes[0]).as("flush() calls").isEqualTo(1);
            assertThat(thrown[0].getSuppressed()).hasSize(1);
            assertThat(thrown[0].getSuppressed()[0])
                    .isInstanceOf(Error.class)
                    .hasMessageContaining("not committed")
                    .hasCause(flushFailure);
            assertNotExist(op, path);
        }
    }

    @Test
    void testOutputStreamRecordsFlushOverrideFailureDuringWrite() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        final String path = "write-flush-override-failure";
        final RuntimeException flushFailure = new RuntimeException("flush failed");
        final int[] flushes = {0};
        try (final Operator op = Operator.of(fs)) {
            final OperatorOutputStream out =
                    new OperatorOutputStream(op, path, 4, WriteOptions.builder().build()) {
                        @Override
                        public synchronized void flush() {
                            flushes[0]++;
                            // Succeed on a later call, so close() flushing again would commit.
                            if (flushes[0] == 1) {
                                throw flushFailure;
                            }
                            super.flush();
                        }
                    };
            out.write(1);
            out.write(2);
            out.write(3);
            assertThatThrownBy(() -> out.write(4))
                    .as("flushing the full buffer")
                    .isSameAs(flushFailure);
            assertThat(flushes[0]).isEqualTo(1);

            // The failure is recorded even though the override never reached the native writer,
            // so later writes are rejected instead of overrunning the buffer array or spinning
            // on a zero-length batch.
            assertThatThrownBy(() -> out.write(5))
                    .as("write(int) after failure")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not committed")
                    .hasCause(flushFailure);
            assertThatThrownBy(() -> out.write(new byte[] {5, 6}))
                    .as("write(byte[]) after failure")
                    .isInstanceOf(IllegalStateException.class)
                    .hasCause(flushFailure);

            // close() discards without flushing again, even though the override would now succeed.
            assertThatThrownBy(out::close)
                    .as("close after failure")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not committed")
                    .hasCause(flushFailure);
            assertThat(flushes[0]).as("flush() calls").isEqualTo(1);
            assertNotExist(op, path);
        }
    }

    @Test
    void testOutputStreamRejectsUseAfterBufferedFlushFailure() throws Exception {
        try (final AsyncOperator op =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                final AsyncOperator throttled = op.layer(new ThrottleLayer(1024, 4));
                final Operator layeredOp = throttled.blocking()) {
            final String path = "buffered-flush-failure";
            // The buffer is larger than the burst, so filling it and flushing fails while the
            // offset still points past the end of the buffer.
            final OperatorOutputStream out = new OperatorOutputStream(
                    layeredOp, path, 8, WriteOptions.builder().build());
            out.write(new byte[] {1, 2, 3, 4});
            assertThatThrownBy(() -> out.write(new byte[] {5, 6, 7, 8}))
                    .as("flushing the full buffer")
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited));

            // A sticky failure rejects further use instead of overrunning the buffer array or
            // silently accepting bytes that will never be written.
            assertThatThrownBy(() -> out.write(1))
                    .as("write after failure")
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited))
                    .hasCauseInstanceOf(OpenDALException.class);
            assertThatThrownBy(out::flush)
                    .as("flush after failure")
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited))
                    .hasCauseInstanceOf(OpenDALException.class);

            // Committing an empty writer would publish an empty object, so close() discards.
            assertThatThrownBy(out::close)
                    .as("close after failure")
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RateLimited));
            assertNotExist(layeredOp, path);
        }
    }

    @Test
    void testOutputStreamAbortDiscardsStagedWriteWithoutFlushing() throws Exception {
        final Path staging = Files.createTempDirectory(tempDir, "abort-staging");
        final ServiceConfig.Fs fs = ServiceConfig.Fs.builder()
                .root(tempDir.toString())
                .atomicWriteDir(staging.toString())
                .build();
        final String path = "abort-staged";
        final byte[] original = {1, 2, 3};
        try (final Operator op = Operator.of(fs)) {
            op.write(path, original);
            try (final OperatorOutputStream out =
                    new OperatorOutputStream(op, path, 8, WriteOptions.builder().build()) {
                        @Override
                        public synchronized void flush() {
                            throw new AssertionError("abort or automatic close called flush");
                        }
                    }) {
                out.write(new byte[16]);
                out.write(1);
                assertThat(staging).isNotEmptyDirectory();

                out.abort();
                assertThat(staging).isEmptyDirectory();
                assertThatThrownBy(() -> out.write(1)).isInstanceOf(IllegalStateException.class);
                out.abort();
            }
            assertThat(op.read(path)).isEqualTo(original);
        }
    }

    @Test
    void testOutputStreamAbortFailureStillClosesStream() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs);
                final OperatorOutputStream out = op.createOutputStream("abort-unstaged", 8)) {
            out.write(new byte[16]);
            assertThatThrownBy(out::abort).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.Unsupported));
            assertThatThrownBy(() -> out.write(1)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(out::flush).isInstanceOf(IllegalStateException.class);
        }
    }

    private static void assertNotExist(Operator op, String path) {
        assertThatThrownBy(() -> op.stat(path)).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testInvalidOutputBufferSize(int bufferSize) {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            assertThatThrownBy(() -> op.createOutputStream("invalid-buffer", bufferSize))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBytes");
        }
    }

    @Test
    void testBulkWriteArgumentChecks() throws Exception {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            try (final OperatorOutputStream out = op.createOutputStream("bounds")) {
                final byte[] content = {1, 2, 3};
                assertThatThrownBy(() -> out.write(null, 0, 0)).isInstanceOf(NullPointerException.class);
                assertThatThrownBy(() -> out.write(content, -1, 1)).isInstanceOf(IndexOutOfBoundsException.class);
                assertThatThrownBy(() -> out.write(content, 0, -1)).isInstanceOf(IndexOutOfBoundsException.class);
                assertThatThrownBy(() -> out.write(content, 2, 2)).isInstanceOf(IndexOutOfBoundsException.class);
                assertThatThrownBy(() -> out.write(content, Integer.MAX_VALUE, 1))
                        .isInstanceOf(IndexOutOfBoundsException.class);
                out.write(content, content.length, 0);
                out.write(content);
            }
            assertThat(op.read("bounds")).isEqualTo(new byte[] {1, 2, 3});
        }
    }

    @Test
    void testCreateOutputStreamWithOptions() {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();
        try (final Operator op = Operator.of(fs)) {
            final String path = "testCreateOutputStreamWithOptions.txt";
            final String content = "0123456789";

            try (final OperatorOutputStream os = op.createOutputStream(
                    path, WriteOptions.builder().contentType("text/plain").build())) {
                os.write(content.getBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            assertThat(op.read(path)).isEqualTo(content.getBytes());
        }
    }
}
