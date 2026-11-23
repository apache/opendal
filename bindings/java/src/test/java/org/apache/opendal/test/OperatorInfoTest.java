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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Capability;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorInfo;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.layer.CapabilityOverrideLayer;
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

            assertThat(info.capability).isNotNull();
            assertThat(info.capability.read).isTrue();
            assertThat(info.capability.write).isTrue();
            assertThat(info.capability.delete).isTrue();
            assertThat(info.capability.writeCanAppend).isTrue();
            assertThat(info.capability.writeMultiMaxSize).isEqualTo(-1);
            assertThat(info.capability.writeMultiMinSize).isEqualTo(-1);
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

            assertThat(info.capability).isNotNull();
            assertThat(info.capability.read).isTrue();
            assertThat(info.capability.write).isTrue();
            assertThat(info.capability.delete).isTrue();
            assertThat(info.capability.writeCanAppend).isFalse();
            assertThat(info.capability.writeMultiMaxSize).isEqualTo(-1);
            assertThat(info.capability.writeMultiMinSize).isEqualTo(-1);
        }
    }

    @Test
    public void testStatVersionConditionCapabilities() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        try (final AsyncOperator op = AsyncOperator.of(memory);
                final AsyncOperator matchOp = op.layer(new CapabilityOverrideLayer("stat_with_if_version_match=true"));
                final AsyncOperator notMatchOp =
                        op.layer(new CapabilityOverrideLayer("stat_with_if_version_not_match=true"))) {
            assertThat(op.info.capability.statWithIfVersionMatch).isFalse();
            assertThat(op.info.capability.statWithIfVersionNotMatch).isFalse();

            assertThat(matchOp.info.capability.statWithIfVersionMatch).isTrue();
            assertThat(matchOp.info.capability.statWithIfVersionNotMatch).isFalse();

            assertThat(notMatchOp.info.capability.statWithIfVersionMatch).isFalse();
            assertThat(notMatchOp.info.capability.statWithIfVersionNotMatch).isTrue();
        }
    }

    @Test
    public void testReadConditionCapabilities() {
        String[] flags = {
            "read_with_version",
            "read_with_if_version_match",
            "read_with_if_version_not_match",
            "read_with_if_modified_since",
            "read_with_if_unmodified_since"
        };
        String allDisabled = String.join("=false,", flags) + "=false";
        try (AsyncOperator base =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                AsyncOperator none = base.layer(new CapabilityOverrideLayer(allDisabled))) {
            for (int i = 0; i < flags.length; i++) {
                boolean[] expected = new boolean[flags.length];
                expected[i] = true;
                try (AsyncOperator enabled = none.layer(new CapabilityOverrideLayer(flags[i] + "=true"));
                        AsyncOperator disabled = enabled.layer(new CapabilityOverrideLayer(flags[i] + "=false"))) {
                    Capability enabledCapability = enabled.info.capability;
                    assertThat(new boolean[] {
                                enabledCapability.readWithVersion,
                                enabledCapability.readWithIfVersionMatch,
                                enabledCapability.readWithIfVersionNotMatch,
                                enabledCapability.readWithIfModifiedSince,
                                enabledCapability.readWithIfUnmodifiedSince
                            })
                            .as("enabling %s", flags[i])
                            .containsExactly(expected);
                    Capability disabledCapability = disabled.info.capability;
                    assertThat(new boolean[] {
                                disabledCapability.readWithVersion,
                                disabledCapability.readWithIfVersionMatch,
                                disabledCapability.readWithIfVersionNotMatch,
                                disabledCapability.readWithIfModifiedSince,
                                disabledCapability.readWithIfUnmodifiedSince
                            })
                            .as("disabling %s", flags[i])
                            .containsOnly(false);
                }
            }
        }
    }

    @Test
    public void testAllGeneratedCapabilityFieldsMatchCoreOverrides() throws Exception {
        try (AsyncOperator base =
                AsyncOperator.of(ServiceConfig.Memory.builder().build())) {
            for (Field field : Capability.class.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                String snakeName = field.getName().replaceAll("([A-Z])", "_$1").toLowerCase(Locale.ROOT);
                if (field.getType() == boolean.class) {
                    try (AsyncOperator enabled = base.layer(new CapabilityOverrideLayer(snakeName + "=true"));
                            AsyncOperator disabled = base.layer(new CapabilityOverrideLayer(snakeName + "=false"))) {
                        assertThat(field.getBoolean(enabled.info.capability))
                                .as("enabling %s", snakeName)
                                .isTrue();
                        assertThat(field.getBoolean(disabled.info.capability))
                                .as("disabling %s", snakeName)
                                .isFalse();
                    }
                } else if (field.getType() == long.class) {
                    try (AsyncOperator set = base.layer(new CapabilityOverrideLayer(snakeName + "=4096"));
                            AsyncOperator unset = set.layer(new CapabilityOverrideLayer(snakeName + "=none"))) {
                        assertThat(field.getLong(set.info.capability))
                                .as("setting %s", snakeName)
                                .isEqualTo(4096L);
                        assertThat(field.getLong(unset.info.capability))
                                .as("unsetting %s", snakeName)
                                .isEqualTo(-1L);
                    }
                }
            }
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCapabilityBuilderAndLegacyConstructors() {
        Capability defaults = Capability.builder().build();
        assertThat(defaults.stat).isFalse();
        assertThat(defaults.read).isFalse();
        assertThat(defaults.write).isFalse();
        assertThat(defaults.writeMultiMaxSize).isEqualTo(-1L);
        assertThat(defaults.writeMultiMinSize).isEqualTo(-1L);
        assertThat(defaults.writeTotalMaxSize).isEqualTo(-1L);
        assertThat(defaults.deleteMaxSize).isEqualTo(-1L);
        assertThat(defaults.copyMultiMaxSize).isEqualTo(-1L);
        assertThat(defaults.copyMultiMinSize).isEqualTo(-1L);

        Capability modified = defaults.toBuilder()
                .read(true)
                .listWithGlob(true)
                .writeWithExpires(true)
                .deleteWithVersion(true)
                .presignDelete(true)
                .deleteMaxSize(1000L)
                .build();
        assertThat(modified.read).isTrue();
        assertThat(modified.listWithGlob).isTrue();
        assertThat(modified.writeWithExpires).isTrue();
        assertThat(modified.deleteWithVersion).isTrue();
        assertThat(modified.presignDelete).isTrue();
        assertThat(modified.deleteMaxSize).isEqualTo(1000L);
        assertThat(modified.writeMultiMaxSize).isEqualTo(-1L);

        Capability legacy41 = new Capability(
                true, false, false, false, false, false, false, false, true, false, false, false, false, false, true,
                false, false, false, false, false, false, false, false, false, 1024L, 512L, true, true, false, false,
                true, false, false, false, false, false, false, false, false, false, true);
        assertThat(legacy41.stat).isTrue();
        assertThat(legacy41.read).isTrue();
        assertThat(legacy41.write).isTrue();
        assertThat(legacy41.writeMultiMaxSize).isEqualTo(1024L);
        assertThat(legacy41.writeMultiMinSize).isEqualTo(512L);
        assertThat(legacy41.writeTotalMaxSize).isEqualTo(-1L);
        assertThat(legacy41.readWithVersion).isFalse();
        assertThat(legacy41.shared).isTrue();

        Capability legacy46 = new Capability(
                true, false, false, false, false, false, false, false, true, false, false, false, false, false, true,
                false, false, false, false, false, false, false, false, false, 2048L, 1024L, true, true, false, false,
                true, false, false, false, false, false, false, false, false, false, true, true, true, false, true,
                false);
        assertThat(legacy46.writeMultiMaxSize).isEqualTo(2048L);
        assertThat(legacy46.writeMultiMinSize).isEqualTo(1024L);
        assertThat(legacy46.writeTotalMaxSize).isEqualTo(-1L);
        assertThat(legacy46.readWithVersion).isTrue();
        assertThat(legacy46.readWithIfVersionMatch).isTrue();
        assertThat(legacy46.readWithIfVersionNotMatch).isFalse();
        assertThat(legacy46.readWithIfModifiedSince).isTrue();
        assertThat(legacy46.readWithIfUnmodifiedSince).isFalse();
        assertThat(legacy46.listWithGlob).isFalse();
    }
}
