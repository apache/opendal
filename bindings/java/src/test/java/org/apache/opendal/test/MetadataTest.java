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

import static org.apache.opendal.test.behavior.BehaviorTestBase.generateBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Entry;
import org.apache.opendal.ListOptions;
import org.apache.opendal.Metadata;
import org.apache.opendal.Metadata.EntryMode;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.WriteOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

public class MetadataTest {
    private static final Instant MODIFIED = Instant.parse("2024-07-15T01:02:03.456789Z");

    @TempDir
    private static Path tempDir;

    @Test
    public void testAsyncMetadata() {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();

        try (final AsyncOperator op = AsyncOperator.of(fs)) {
            final String dir = UUID.randomUUID() + "/";
            op.createDir(dir).join();
            final Metadata dirMetadata = op.stat(dir).join();
            assertTrue(dirMetadata.isDir());

            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            op.write(path, content).join();

            final Metadata metadata = op.stat(path).join();
            assertTrue(metadata.isFile());
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.lastModified).isNotNull();
            assertThat(metadata.cacheControl).isNull();
            assertThat(metadata.contentDisposition).isNull();
            assertThat(metadata.contentEncoding).isNull();
            assertThat(metadata.contentLanguage).isNull();
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.contentType).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.version).isNull();
            assertThat(metadata.current).isNull();
            assertThat(metadata.deleted).isFalse();
            assertThat(metadata.userMetadata).isEmpty();

            op.delete(dir).join();
            op.delete(path).join();
        }
    }

    @Test
    public void testBlockingMetadata() {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();

        try (final Operator op = Operator.of(fs)) {
            final String dir = UUID.randomUUID() + "/";
            op.createDir(dir);
            final Metadata dirMetadata = op.stat(dir);
            assertTrue(dirMetadata.isDir());

            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            op.write(path, content);

            final Metadata metadata = op.stat(path);
            assertTrue(metadata.isFile());
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.lastModified).isNotNull();
            assertThat(metadata.cacheControl).isNull();
            assertThat(metadata.contentDisposition).isNull();
            assertThat(metadata.contentEncoding).isNull();
            assertThat(metadata.contentLanguage).isNull();
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.contentType).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.version).isNull();
            assertThat(metadata.current).isNull();
            assertThat(metadata.deleted).isFalse();
            assertThat(metadata.userMetadata).isEmpty();

            op.delete(dir);
            op.delete(path);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testExistingConstructors() {
        final Metadata file = new Metadata(
                0,
                5_000_000_000L,
                "attachment",
                "example-md5",
                "text/plain",
                "max-age=60",
                "example-etag",
                MODIFIED,
                "example-version");
        assertConstructorValues(file, EntryMode.FILE);
        assertThat(file.userMetadata).isEmpty();
        assertThat(file.getUserMetadata()).isEmpty();

        final Map<String, String> custom = Collections.singletonMap("owner", "opendal");
        final Metadata dir = new Metadata(
                1,
                5_000_000_000L,
                "attachment",
                "example-md5",
                "text/plain",
                "max-age=60",
                "example-etag",
                MODIFIED,
                "example-version",
                custom);
        assertConstructorValues(dir, EntryMode.DIR);
        assertThat(dir.userMetadata).isSameAs(custom);
        assertThat(dir.getUserMetadata()).isSameAs(custom);

        final Metadata unknown = new Metadata(2, 0L, null, null, null, null, null, null, null, null);
        assertThat(unknown.mode).isEqualTo(EntryMode.UNKNOWN);
        assertThat(unknown.isFile()).isFalse();
        assertThat(unknown.isDir()).isFalse();
        assertThat(unknown.userMetadata).isNull();
        assertThat(unknown.getUserMetadata()).isNull();
    }

    private static void assertConstructorValues(Metadata metadata, EntryMode mode) {
        assertThat(metadata.mode).isEqualTo(mode);
        assertThat(metadata.getMode()).isEqualTo(metadata.mode);
        assertThat(metadata.contentLength).isEqualTo(5_000_000_000L);
        assertThat(metadata.getContentLength()).isEqualTo(metadata.contentLength);
        assertThat(metadata.contentDisposition).isEqualTo("attachment");
        assertThat(metadata.getContentDisposition()).isEqualTo(metadata.contentDisposition);
        assertThat(metadata.contentMd5).isEqualTo("example-md5");
        assertThat(metadata.getContentMd5()).isEqualTo(metadata.contentMd5);
        assertThat(metadata.contentType).isEqualTo("text/plain");
        assertThat(metadata.getContentType()).isEqualTo(metadata.contentType);
        assertThat(metadata.cacheControl).isEqualTo("max-age=60");
        assertThat(metadata.getCacheControl()).isEqualTo(metadata.cacheControl);
        assertThat(metadata.etag).isEqualTo("example-etag");
        assertThat(metadata.getEtag()).isEqualTo(metadata.etag);
        assertThat(metadata.lastModified).isEqualTo(MODIFIED);
        assertThat(metadata.getLastModified()).isEqualTo(metadata.lastModified);
        assertThat(metadata.version).isEqualTo("example-version");
        assertThat(metadata.getVersion()).isEqualTo(metadata.version);
        assertThat(metadata.current).isNull();
        assertThat(metadata.deleted).isFalse();
        assertThat(metadata.contentEncoding).isNull();
        assertThat(metadata.contentLanguage).isNull();
        assertThat(metadata.isFile()).isEqualTo(mode == EntryMode.FILE);
        assertThat(metadata.isDir()).isEqualTo(mode == EntryMode.DIR);
    }

    @Test
    public void testEntryMode() {
        assertThat(EntryMode.values()).containsExactly(EntryMode.FILE, EntryMode.DIR, EntryMode.UNKNOWN);
        assertThat(EntryMode.of(0)).isEqualTo(EntryMode.FILE);
        assertThat(EntryMode.of(1)).isEqualTo(EntryMode.DIR);
        assertThat(EntryMode.of(2)).isEqualTo(EntryMode.UNKNOWN);
        assertThatThrownBy(() -> EntryMode.of(3)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPublicMetadataShape() throws ReflectiveOperationException {
        final Class<?>[] withoutUserMetadata = {
            int.class,
            long.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            Instant.class,
            String.class
        };
        final Class<?>[] withUserMetadata = Arrays.copyOf(withoutUserMetadata, 10);
        withUserMetadata[9] = Map.class;
        assertThat(Metadata.class.getConstructors())
                .containsExactlyInAnyOrder(
                        Metadata.class.getConstructor(withoutUserMetadata),
                        Metadata.class.getConstructor(withUserMetadata))
                .allSatisfy(constructor -> assertThat(constructor.isAnnotationPresent(Deprecated.class))
                        .isTrue());

        final Field[] fields = Metadata.class.getDeclaredFields();
        assertThat(fields)
                .extracting(Field::getName, Field::getType)
                .containsExactlyInAnyOrder(
                        tuple("mode", EntryMode.class),
                        tuple("current", Boolean.class),
                        tuple("deleted", boolean.class),
                        tuple("cacheControl", String.class),
                        tuple("contentLength", long.class),
                        tuple("contentMd5", String.class),
                        tuple("contentType", String.class),
                        tuple("contentEncoding", String.class),
                        tuple("contentLanguage", String.class),
                        tuple("lastModified", Instant.class),
                        tuple("etag", String.class),
                        tuple("contentDisposition", String.class),
                        tuple("version", String.class),
                        tuple("userMetadata", Map.class));
        assertThat(fields)
                .allSatisfy(field -> assertThat(field.getModifiers()).isEqualTo(Modifier.PUBLIC | Modifier.FINAL));
        assertThat(Metadata.class.getField("userMetadata").getGenericType().getTypeName())
                .isEqualTo("java.util.Map<java.lang.String, java.lang.String>");
    }

    @Test
    public void testBuilderAndToBuilder() {
        assertThatThrownBy(() -> Metadata.builder().mode(null)).isInstanceOf(NullPointerException.class);

        final Metadata defaults = Metadata.builder().build();
        assertThat(defaults.getMode()).isEqualTo(EntryMode.UNKNOWN);
        assertThat(defaults.getCurrent()).isNull();
        assertThat(defaults.isDeleted()).isFalse();
        assertThat(defaults.getCacheControl()).isNull();
        assertThat(defaults.getContentLength()).isZero();
        assertThat(defaults.getContentMd5()).isNull();
        assertThat(defaults.getContentType()).isNull();
        assertThat(defaults.getContentEncoding()).isNull();
        assertThat(defaults.getContentLanguage()).isNull();
        assertThat(defaults.getLastModified()).isNull();
        assertThat(defaults.getEtag()).isNull();
        assertThat(defaults.getContentDisposition()).isNull();
        assertThat(defaults.getVersion()).isNull();
        assertThat(defaults.getUserMetadata()).isEmpty();

        final Map<String, String> custom = Collections.singletonMap("owner", "opendal");
        final Metadata metadata = Metadata.builder()
                .mode(EntryMode.FILE)
                .current(Boolean.FALSE)
                .deleted(true)
                .cacheControl("max-age=60")
                .contentLength(5_000_000_000L)
                .contentMd5("builder-md5")
                .contentType("text/plain")
                .contentEncoding("zstd")
                .contentLanguage("en-US")
                .lastModified(MODIFIED)
                .etag("builder-etag")
                .contentDisposition("attachment")
                .version("builder-version")
                .userMetadata(custom)
                .build();
        assertThat(metadata.getMode()).isEqualTo(EntryMode.FILE);
        assertThat(metadata.getCurrent()).isEqualTo(Boolean.FALSE);
        assertThat(metadata.isDeleted()).isTrue();
        assertThat(metadata.getCacheControl()).isEqualTo("max-age=60");
        assertThat(metadata.getContentLength()).isEqualTo(5_000_000_000L);
        assertThat(metadata.getContentMd5()).isEqualTo("builder-md5");
        assertThat(metadata.getContentType()).isEqualTo("text/plain");
        assertThat(metadata.getContentEncoding()).isEqualTo("zstd");
        assertThat(metadata.getContentLanguage()).isEqualTo("en-US");
        assertThat(metadata.getLastModified()).isEqualTo(MODIFIED);
        assertThat(metadata.getEtag()).isEqualTo("builder-etag");
        assertThat(metadata.getContentDisposition()).isEqualTo("attachment");
        assertThat(metadata.getVersion()).isEqualTo("builder-version");
        assertThat(metadata.getUserMetadata()).isSameAs(custom);
        assertThat(metadata.toBuilder().build()).isEqualTo(metadata);
        assertThatThrownBy(() -> metadata.toBuilder().mode(null)).isInstanceOf(NullPointerException.class);

        final Metadata updated = metadata.toBuilder()
                .current(null)
                .contentEncoding(null)
                .version("next-version")
                .userMetadata(null)
                .build();
        assertThat(updated.getCurrent()).isNull();
        assertThat(updated.getContentEncoding()).isNull();
        assertThat(updated.getVersion()).isEqualTo("next-version");
        assertThat(updated.getUserMetadata()).isNull();
        assertThat(updated.toBuilder().build()).isEqualTo(updated);
        assertThat(metadata.getVersion()).isEqualTo("builder-version");
        assertThat(metadata.getUserMetadata()).isSameAs(custom);
    }

    @Test
    public void testMemoryMetadataRoundTrip() {
        final byte[] content = {1, 2, 3};
        final WriteOptions options = WriteOptions.builder()
                .contentDisposition("attachment; filename=report.txt")
                .contentEncoding("gzip")
                .contentType("application/octet-stream")
                .cacheControl("no-cache")
                .userMetadata(Collections.singletonMap("owner", "opendal"))
                .build();

        try (final Operator op = Operator.of(ServiceConfig.Memory.builder().build())) {
            op.write("report.txt", content, options);
            final Metadata metadata = op.stat("report.txt");
            assertThat(metadata.mode).isEqualTo(EntryMode.FILE);
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.contentDisposition).isEqualTo(options.contentDisposition);
            assertThat(metadata.contentEncoding).isEqualTo(options.contentEncoding);
            assertThat(metadata.contentLanguage).isNull();
            assertThat(metadata.contentType).isEqualTo(options.contentType);
            assertThat(metadata.cacheControl).isEqualTo(options.cacheControl);
            assertThat(metadata.userMetadata).isEqualTo(options.userMetadata);
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.lastModified).isNull();
            assertThat(metadata.version).isNull();
            assertThat(metadata.current).isNull();
            assertThat(metadata.deleted).isFalse();
        }
    }

    @Test
    @Timeout(10)
    public void testContentHeadersAndVersionedListingMetadata() throws IOException {
        final byte[] versions = ("<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                        + "<Name>bucket</Name><Prefix>dir/</Prefix><IsTruncated>false</IsTruncated>"
                        + "<Version><Key>dir/file</Key><VersionId>v2</VersionId><IsLatest>true</IsLatest>"
                        + "<LastModified>2024-01-02T00:00:00Z</LastModified><ETag>&quot;etag-v2&quot;</ETag>"
                        + "<Size>5</Size></Version>"
                        + "<Version><Key>dir/file</Key><VersionId>v1</VersionId><IsLatest>false</IsLatest>"
                        + "<LastModified>2024-01-01T00:00:00Z</LastModified><ETag>&quot;etag-v1&quot;</ETag>"
                        + "<Size>3</Size></Version>"
                        + "<DeleteMarker><Key>dir/deleted</Key><VersionId>dm-2</VersionId><IsLatest>true</IsLatest>"
                        + "<LastModified>2024-01-03T00:00:00Z</LastModified></DeleteMarker>"
                        + "<DeleteMarker><Key>dir/deleted</Key><VersionId>dm-1</VersionId><IsLatest>false</IsLatest>"
                        + "<LastModified>2024-01-01T00:00:00Z</LastModified></DeleteMarker>"
                        + "</ListVersionsResult>")
                .getBytes(StandardCharsets.UTF_8);
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/bucket/dir/file", exchange -> {
            try {
                exchange.getRequestBody().close();
                if (!"HEAD".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                exchange.getResponseHeaders().set("Content-Length", "5");
                exchange.getResponseHeaders().set("Content-Encoding", "zstd");
                exchange.getResponseHeaders().set("Content-Language", "en-US, zh-CN");
                exchange.getResponseHeaders().set("ETag", "\"etag-v2\"");
                exchange.getResponseHeaders().set("x-amz-version-id", "v2");
                exchange.sendResponseHeaders(200, -1);
            } finally {
                exchange.close();
            }
        });
        server.createContext("/bucket", exchange -> {
            try {
                exchange.getRequestBody().close();
                final String query = exchange.getRequestURI().getRawQuery();
                if (!"GET".equals(exchange.getRequestMethod())
                        || query == null
                        || !Arrays.asList(query.split("&")).contains("versions")) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/xml");
                exchange.sendResponseHeaders(200, versions.length);
                exchange.getResponseBody().write(versions);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try (final Operator op = Operator.of(ServiceConfig.S3
                .builder()
                .bucket("bucket")
                .region("us-east-1")
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .skipSignature(true)
                .enableVersioning(true)
                .build())) {
            final Metadata metadata = op.stat("dir/file");
            assertThat(metadata.getContentLength()).isEqualTo(5L);
            assertThat(metadata.getContentEncoding()).isEqualTo("zstd");
            assertThat(metadata.getContentLanguage()).isEqualTo("en-US, zh-CN");
            assertThat(metadata.getEtag()).isEqualTo("\"etag-v2\"");
            assertThat(metadata.getVersion()).isEqualTo("v2");
            assertThat(metadata.getCurrent()).isNull();
            assertThat(metadata.isDeleted()).isFalse();

            final ListOptions options =
                    ListOptions.builder().versions(true).deleted(true).build();
            assertThat(op.list("dir/", options))
                    .extracting(
                            Entry::getPath,
                            entry -> entry.metadata.getVersion(),
                            entry -> entry.metadata.getCurrent(),
                            entry -> entry.metadata.isDeleted())
                    .containsExactlyInAnyOrder(
                            tuple("dir/file", "v2", Boolean.TRUE, false),
                            tuple("dir/file", "v1", Boolean.FALSE, false),
                            tuple("dir/deleted", "dm-2", Boolean.TRUE, true),
                            tuple("dir/deleted", "dm-1", Boolean.FALSE, true));
        } finally {
            server.stop(0);
        }
    }
}
