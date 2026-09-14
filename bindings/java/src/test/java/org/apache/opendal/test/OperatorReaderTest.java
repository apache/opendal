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
import java.nio.file.Path;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorReader;
import org.apache.opendal.ReadOptions;
import org.apache.opendal.ReaderOptions;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class OperatorReaderTest {
    @TempDir
    private Path tempDir;

    @Test
    void testReusableReaderOutlivesOperator() {
        final OperatorReader reader;
        try (Operator op =
                Operator.of(ServiceConfig.Fs.builder().root(tempDir.toString()).build())) {
            op.write("file", "0123456789");
            reader = op.reader(
                    "file",
                    ReaderOptions.builder()
                            .concurrent(2)
                            .chunk(2)
                            .prefetch(1)
                            .contentLengthHint(10)
                            .build());
        }
        try (OperatorReader r = reader) {
            assertThat(r.read(4, 5)).isEqualTo("45678".getBytes(StandardCharsets.UTF_8));
            assertThat(r.read(ReadOptions.builder().offset(1).length(2).build()))
                    .isEqualTo("12".getBytes(StandardCharsets.UTF_8));
            assertThat(r.read(8, -1)).isEqualTo("89".getBytes(StandardCharsets.UTF_8));
            assertThat(r.read(0, 0)).isEmpty();
            assertThat(r.read()).isEqualTo("0123456789".getBytes(StandardCharsets.UTF_8));
        }
        reader.close();
        assertThatThrownBy(reader::read).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @CsvSource({"-1, 1", "0, -2"})
    void testInvalidRange(long offset, long length) {
        try (Operator op = Operator.of(
                        ServiceConfig.Fs.builder().root(tempDir.toString()).build());
                OperatorReader reader = op.reader("missing")) {
            assertThatThrownBy(() -> reader.read(offset, length))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.RangeNotSatisfied));
        }
    }

    @Test
    void testMissingFileFailsOnRead() {
        try (Operator op = Operator.of(
                        ServiceConfig.Fs.builder().root(tempDir.toString()).build());
                OperatorReader reader = op.reader("missing")) {
            assertThatThrownBy(reader::read).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
        }
    }

    @Test
    void testInvalidReaderOptions() {
        try (Operator op =
                Operator.of(ServiceConfig.Fs.builder().root(tempDir.toString()).build())) {
            assertThatThrownBy(() -> op.reader(
                            "missing", ReaderOptions.builder().chunk(0).build()))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid));
        }
    }
}
