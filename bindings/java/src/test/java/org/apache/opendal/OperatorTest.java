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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.opendal.condition.OpenDALExceptionCondition;
import org.apache.opendal.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class OperatorTest {

    private static final List<Operator> ops = new ArrayList<>();

    private static final List<BlockingOperator> blockingOps = new ArrayList<>();

    protected static final String[] schemas = new String[] {
        "atomicserver",
        "azblob",
        "azdls",
        "cacache",
        "cos",
        "dashmap",
        "etcd",
        "foundationdb",
        "fs",
        "ftp",
        "gcs",
        "ghac",
        "hdfs",
        "http",
        "ipfs",
        "ipmfs",
        "memcached",
        "memory",
        "minimoka",
        "moka",
        "obs",
        "onedrive",
        "gdrive",
        "dropbox",
        "oss",
        "persy",
        "redis",
        "postgresql",
        "rocksdb",
        "s3",
        "sftp",
        "sled",
        "supabase",
        "vercel-artifacts",
        "wasabi",
        "webdav",
        "webhdfs",
        "redb",
        "tikv",
    };

    @BeforeAll
    public static void init() {
        for (String schema : schemas) {

            Optional<Operator> opOptional = Utils.init(schema);
            opOptional.ifPresent(op -> ops.add(op));

            Optional<BlockingOperator> blockingOpOptional = Utils.initBlockingOp(schema);
            blockingOpOptional.ifPresent(op -> blockingOps.add(op));
        }
        if (ops.isEmpty()) {
            ops.add(null);
        }
        if (blockingOps.isEmpty()) {
            blockingOps.add(null);
        }
    }

    @AfterAll
    public static void clean() {
        ops.stream().filter(Objects::nonNull).forEach(Operator::close);
        blockingOps.stream().filter(Objects::nonNull).forEach(BlockingOperator::close);
    }

    private static Stream<Operator> getOperators() {
        return ops.stream();
    }

    private static Stream<BlockingOperator> getBlockingOperators() {
        return blockingOps.stream();
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("getBlockingOperators")
    public void testBlockingWrite(BlockingOperator blockingOp) {
        assumeTrue(blockingOp != null);

        Capability cap = blockingOp.info().fullCapability;
        if (!cap.write || !cap.read) {
            return;
        }

        String path = UUID.randomUUID().toString();
        byte[] content = Utils.generateBytes();
        blockingOp.write(path, content);

        Metadata metadata = blockingOp.stat(path);

        assertEquals(content.length, metadata.getContentLength());

        blockingOp.delete(path);
        assertThatThrownBy(() -> blockingOp.stat(path))
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("getBlockingOperators")
    public void testBlockingRead(BlockingOperator blockingOp) {
        assumeTrue(blockingOp != null);

        Capability cap = blockingOp.info().fullCapability;
        if (!cap.write || !cap.read) {
            return;
        }

        Metadata metadata = blockingOp.stat("");
        assertTrue(!metadata.isFile());

        String path = UUID.randomUUID().toString();
        assertThatThrownBy(() -> blockingOp.stat(path))
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));

        byte[] content = Utils.generateBytes();
        blockingOp.write(path, content);

        assertThat(blockingOp.read(path)).isEqualTo(content);

        blockingOp.delete(path);
        assertThatThrownBy(() -> blockingOp.stat(path))
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("getOperators")
    public final void testWrite(Operator op) throws Exception {
        assumeTrue(op != null);

        Capability cap = op.info().fullCapability;
        if (!cap.write || !cap.read) {
            return;
        }

        String path = UUID.randomUUID().toString();
        byte[] content = Utils.generateBytes();
        op.write(path, content).join();

        Metadata metadata = op.stat(path).get();

        assertEquals(content.length, metadata.getContentLength());

        op.delete(path).join();
        assertThatThrownBy(() -> op.stat(path).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("getOperators")
    public final void testRead(Operator op) throws Exception {
        assumeTrue(op != null);

        Capability cap = op.info().fullCapability;
        if (!cap.write || !cap.read) {
            return;
        }

        Metadata metadata = op.stat("").get();
        assertTrue(!metadata.isFile());

        String path = UUID.randomUUID().toString();
        assertThatThrownBy(() -> op.stat(path).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));

        byte[] content = Utils.generateBytes();
        op.write(path, content).join();

        assertThat(op.read(path).join()).isEqualTo(content);

        op.delete(path).join();
        assertThatThrownBy(() -> op.stat(path).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("getOperators")
    public void testAppend(Operator op) {
        assumeTrue(op != null);

        Capability cap = op.info().fullCapability;
        if (!cap.write || !cap.writeCanAppend || !cap.read) {
            return;
        }

        String path = UUID.randomUUID().toString();
        byte[][] trunks = new byte[][] {Utils.generateBytes(), Utils.generateBytes(), Utils.generateBytes()};

        for (int i = 0; i < trunks.length; i++) {
            op.append(path, trunks[i]).join();

            byte[] expected = Arrays.stream(trunks).limit(i + 1).reduce(new byte[0], (arr1, arr2) -> {
                byte[] result = new byte[arr1.length + arr2.length];
                System.arraycopy(arr1, 0, result, 0, arr1.length);
                System.arraycopy(arr2, 0, result, arr1.length, arr2.length);
                return result;
            });

            assertThat(op.read(path).join()).isEqualTo(expected);
        }

        // write overwrite existing content
        byte[] newAttempt = Utils.generateBytes();
        op.write(path, newAttempt).join();
        assertThat(op.read(path).join()).isEqualTo(newAttempt);

        for (int i = 0; i < trunks.length; i++) {
            op.append(path, trunks[i]).join();

            byte[] expected = Stream.concat(
                            Stream.of(newAttempt), Arrays.stream(trunks).limit(i + 1))
                    .reduce(new byte[0], (arr1, arr2) -> {
                        byte[] result = new byte[arr1.length + arr2.length];
                        System.arraycopy(arr1, 0, result, 0, arr1.length);
                        System.arraycopy(arr2, 0, result, arr1.length, arr2.length);
                        return result;
                    });

            assertThat(op.read(path).join()).isEqualTo(expected);
        }
    }
}
