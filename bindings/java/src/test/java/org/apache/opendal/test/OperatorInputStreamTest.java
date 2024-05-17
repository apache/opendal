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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.opendal.BlockingOperator;
import org.apache.opendal.OperatorInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OperatorInputStreamTest {
    @TempDir
    private static Path tempDir;

    @Test
    void testReadWithInputStream() throws Exception {
        final Map<String, String> conf = new HashMap<>();
        conf.put("root", tempDir.toString());

        try (final BlockingOperator op = BlockingOperator.of("fs", conf)) {
            final String path = "OperatorInputStreamTest.txt";
            final StringBuilder content = new StringBuilder();

            final long multi = 1024;
            for (long i = 0; i < multi; i++) {
                content.append("[content] OperatorInputStreamTest\n");
            }
            op.write(path, content.toString().getBytes());

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
