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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.ServiceConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

public class AsyncOperatorCancellationTest {
    @TempDir
    Path tempDir;

    @Test
    @Timeout(60)
    void cancelledAndCompletedFuturesDoNotPanicWorker() throws Exception {
        // The failure happens on a native worker thread, so run the scenario in a
        // child JVM and inspect its output. The child only exits after its Tokio
        // runtime shuts down, which happens after the in-flight tasks complete.
        final String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        final Path java = Paths.get(System.getProperty("java.home"), "bin", executable);
        final Path log = tempDir.resolve("async-completion.log");
        final Process process = new ProcessBuilder(
                        java.toString(),
                        "-Xcheck:jni",
                        "-Djava.library.path=" + System.getProperty("java.library.path"),
                        "-cp",
                        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                        Application.class.getName())
                .directory(tempDir.toFile())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        final boolean exited;
        try {
            exited = process.waitFor(45, TimeUnit.SECONDS);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
        final String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
        assertThat(exited)
                .as("child JVM must exit after the native tasks finish: %s", output)
                .isTrue();
        assertThat(process.exitValue()).as(output).isZero();
        assertThat(output).contains("ASYNC_COMPLETION_CHECKS_PASSED");
        assertThat(output).doesNotContain("panicked at");
        assertThat(output).doesNotContain("complete future must succeed");
    }

    public static class Application {
        public static void main(String[] args) throws Exception {
            final CountDownLatch cancelledRequested = new CountDownLatch(1);
            final CountDownLatch completedRequested = new CountDownLatch(1);
            final CountDownLatch releaseCancelled = new CountDownLatch(1);
            final CountDownLatch releaseCompleted = new CountDownLatch(1);
            final CountDownLatch cancelledResponseSent = new CountDownLatch(1);
            final CountDownLatch completedResponseSent = new CountDownLatch(1);

            final ExecutorService serverExecutor = Executors.newCachedThreadPool();
            final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(serverExecutor);
            server.createContext(
                    "/cancelled.txt",
                    exchange ->
                            respondWhenReleased(exchange, cancelledRequested, releaseCancelled, cancelledResponseSent));
            server.createContext(
                    "/completed.txt",
                    exchange ->
                            respondWhenReleased(exchange, completedRequested, releaseCompleted, completedResponseSent));
            server.createContext("/after.txt", exchange -> respondImmediately(exchange, "after"));
            server.start();
            try {
                final ServiceConfig.Http config = ServiceConfig.Http.builder()
                        .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                        .build();
                final AsyncExecutor executor = AsyncExecutor.createTokioExecutor(1);
                final AsyncOperator op = AsyncOperator.of(config, executor);
                try {
                    final CompletableFuture<byte[]> cancelled = op.read("cancelled.txt");
                    final CompletableFuture<byte[]> completed = op.read("completed.txt");
                    assertThat(cancelledRequested.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(completedRequested.await(10, TimeUnit.SECONDS)).isTrue();

                    assertThat(cancelled.cancel(true)).isTrue();
                    assertThat(completed.complete("external".getBytes(StandardCharsets.UTF_8)))
                            .isTrue();

                    // The executor and operator remain usable while the cancelled
                    // and externally completed reads are still in flight.
                    assertThat(op.read("after.txt").get(10, TimeUnit.SECONDS))
                            .isEqualTo("after".getBytes(StandardCharsets.UTF_8));

                    releaseCancelled.countDown();
                    releaseCompleted.countDown();
                    assertThat(cancelledResponseSent.await(10, TimeUnit.SECONDS))
                            .isTrue();
                    assertThat(completedResponseSent.await(10, TimeUnit.SECONDS))
                            .isTrue();
                } finally {
                    op.close();
                    executor.close();
                }
            } finally {
                server.stop(0);
                serverExecutor.shutdownNow();
            }
            System.out.println("ASYNC_COMPLETION_CHECKS_PASSED");
        }

        private static void respondWhenReleased(
                HttpExchange exchange,
                CountDownLatch requestReceived,
                CountDownLatch release,
                CountDownLatch responseSent)
                throws IOException {
            requestReceived.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting to release the response");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            respondImmediately(exchange, "native response");
            responseSent.countDown();
        }

        private static void respondImmediately(HttpExchange exchange, String content) throws IOException {
            final byte[] body = content.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }
    }
}
