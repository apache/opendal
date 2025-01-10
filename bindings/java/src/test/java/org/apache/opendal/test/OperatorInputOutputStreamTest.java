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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorInputStream;
import org.apache.opendal.OperatorOutputStream;
import org.apache.opendal.ServiceConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
}
