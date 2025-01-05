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
import java.nio.file.Path;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorInfo;
import org.apache.opendal.ServiceConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OperatorInfoTest {
    @TempDir
    private static Path tempDir;

    @Test
    public void testBlockingOperatorInfo() {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();

        try (final Operator op = Operator.of(fs)) {
            final OperatorInfo info = op.info;
            assertThat(info).isNotNull();
            assertThat(info.scheme).isEqualTo("fs");

            assertThat(info.fullCapability).isNotNull();
            assertThat(info.fullCapability.read).isTrue();
            assertThat(info.fullCapability.write).isTrue();
            assertThat(info.fullCapability.delete).isTrue();
            assertThat(info.fullCapability.writeCanAppend).isTrue();
            assertThat(info.fullCapability.writeMultiMaxSize).isEqualTo(-1);
            assertThat(info.fullCapability.writeMultiMinSize).isEqualTo(-1);

            assertThat(info.nativeCapability).isNotNull();
        }
    }

    @Test
    public void testOperatorInfo() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        try (final AsyncOperator op = AsyncOperator.of(memory)) {
            final OperatorInfo info = op.info;
            assertThat(info).isNotNull();
            assertThat(info.scheme).isEqualTo("memory");

            assertThat(info.fullCapability).isNotNull();
            assertThat(info.fullCapability.read).isTrue();
            assertThat(info.fullCapability.write).isTrue();
            assertThat(info.fullCapability.delete).isTrue();
            assertThat(info.fullCapability.writeCanAppend).isFalse();
            assertThat(info.fullCapability.writeMultiMaxSize).isEqualTo(-1);
            assertThat(info.fullCapability.writeMultiMinSize).isEqualTo(-1);

            assertThat(info.nativeCapability).isNotNull();
        }
    }
}
