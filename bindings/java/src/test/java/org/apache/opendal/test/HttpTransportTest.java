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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class HttpTransportTest {
    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @Timeout(10)
    void testDefaultHttpTransport() {
        final byte[] content = "Hello, OpenDAL!".getBytes(StandardCharsets.UTF_8);
        server.createContext("/example.txt", exchange -> {
            try {
                exchange.sendResponseHeaders(200, content.length);
                exchange.getResponseBody().write(content);
            } finally {
                exchange.close();
            }
        });
        final ServiceConfig.Http config = ServiceConfig.Http.builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .build();
        try (final AsyncExecutor executor = AsyncExecutor.createTokioExecutor(1);
                final AsyncOperator async = AsyncOperator.of(config, executor);
                final Operator blocking = async.blocking()) {
            assertThat(async.read("example.txt").join()).isEqualTo(content);
            assertThat(blocking.read("example.txt")).isEqualTo(content);
        }
    }

    @Test
    void testConfiguredHttpTransportWithExplicitExecutor() throws Exception {
        final byte[] content = "Hello from the proxy!".getBytes(StandardCharsets.UTF_8);
        final AtomicReference<URI> requestTarget = new AtomicReference<>();
        server.createContext("/", exchange -> {
            try {
                requestTarget.set(exchange.getRequestURI());
                exchange.sendResponseHeaders(200, content.length);
                exchange.getResponseBody().write(content);
            } finally {
                exchange.close();
            }
        });
        final String endpoint = "http://127.0.0.1:1";
        final Map<String, String> config = new HashMap<>();
        config.put("endpoint", endpoint);
        config.put(
                "context.http.proxy", "http://127.0.0.1:" + server.getAddress().getPort());
        config.put("layers", "tracing,concurrent_limit,timeout");
        config.put("layer.concurrent_limit.permits", "2");
        // Replaying on the composed context would make the HTTP call acquire this permit twice.
        config.put("layer.concurrent_limit.http_permits", "1");
        // Bound the native task as well as the test's wait for completion.
        config.put("layer.timeout.io_timeout", "5s");
        try (final AsyncExecutor executor = AsyncExecutor.createTokioExecutor(1);
                final AsyncOperator async = AsyncOperator.of("http", config, executor)) {
            assertThat(async.read("context-replay.txt").get(10, TimeUnit.SECONDS))
                    .isEqualTo(content);
            assertThat(requestTarget.get()).isEqualTo(URI.create(endpoint + "/context-replay.txt"));
        }
    }

    @ParameterizedTest(name = "blocking={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(10)
    void testWriteChecksumMismatch(boolean blocking) {
        final byte[] response = "<Error><Code>BadDigest</Code><Message>Checksum mismatch</Message></Error>"
                .getBytes(StandardCharsets.UTF_8);
        server.createContext("/checksum-fixture/file.txt", exchange -> {
            try {
                if (!"PUT".equals(exchange.getRequestMethod())
                        || !exchange.getRequestHeaders().containsKey("x-amz-checksum-crc32c")) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/xml");
                exchange.sendResponseHeaders(400, response.length);
                exchange.getResponseBody().write(response);
            } finally {
                exchange.close();
            }
        });
        final ServiceConfig.S3 config = ServiceConfig.S3
                .builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .bucket("checksum-fixture")
                .region("us-east-1")
                .skipSignature(true)
                .disableConfigLoad(true)
                .disableEc2Metadata(true)
                .checksumAlgorithm("crc32c")
                .build();
        try (final AsyncOperator async = AsyncOperator.of(config)) {
            final OpenDALException error;
            if (blocking) {
                try (final Operator operator = async.blocking()) {
                    error = assertThrows(OpenDALException.class, () -> operator.write("file.txt", "data"));
                }
            } else {
                final ExecutionException failure =
                        assertThrows(ExecutionException.class, () -> async.write("file.txt", "data")
                                .get(5, TimeUnit.SECONDS));
                error = assertInstanceOf(OpenDALException.class, failure.getCause());
            }
            assertThat(error).hasMessageContaining("BadDigest");
            assertThat(error.getCode()).isEqualTo(OpenDALException.Code.ChecksumMismatch);
        }
    }
}
