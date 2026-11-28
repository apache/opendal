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
import java.time.Instant;
import java.util.UUID;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.StatOptions;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;

public class StatOptionsTest {

    @Test
    void testSettersAndToBuilder() {
        StatOptions options = StatOptions.builder().build();
        options.setIfMatch("\"etag\"");
        options.setIfNoneMatch("\"other\"");
        options.setIfVersionMatch("v1");
        options.setIfVersionNotMatch("v2");
        options.setIfModifiedSince(Instant.EPOCH);
        options.setIfUnmodifiedSince(Instant.EPOCH.plusSeconds(1));
        options.setVersion("v1");
        options.setOverrideContentType("text/plain");
        options.setOverrideCacheControl("no-cache");
        options.setOverrideContentDisposition("inline");

        assertThat(options.toBuilder().build()).isEqualTo(options);
        StatOptions updated = options.toBuilder().version("v3").build();
        assertThat(updated.getIfMatch()).isEqualTo("\"etag\"");
        assertThat(updated.getIfNoneMatch()).isEqualTo("\"other\"");
        assertThat(updated.getIfVersionMatch()).isEqualTo("v1");
        assertThat(updated.getIfVersionNotMatch()).isEqualTo("v2");
        assertThat(updated.getIfModifiedSince()).isEqualTo(Instant.EPOCH);
        assertThat(updated.getIfUnmodifiedSince()).isEqualTo(Instant.EPOCH.plusSeconds(1));
        assertThat(updated.getVersion()).isEqualTo("v3");
        assertThat(updated.getOverrideContentType()).isEqualTo("text/plain");
        assertThat(updated.getOverrideCacheControl()).isEqualTo("no-cache");
        assertThat(updated.getOverrideContentDisposition()).isEqualTo("inline");
        assertThat(options.getVersion()).isEqualTo("v1");
    }

    /**
     * The memory service supports neither version condition, so core rejects each one as
     * unsupported. A condition dropped by the binding would let the stat succeed instead.
     */
    @Test
    public void testVersionConditionsReachCore() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        try (final Operator op = Operator.of(memory)) {
            assertThat(op.info.capability.statWithIfVersionMatch).isFalse();
            assertThat(op.info.capability.statWithIfVersionNotMatch).isFalse();

            final String path = UUID.randomUUID().toString();
            op.write(path, "content");

            final StatOptions ifVersionMatch =
                    StatOptions.builder().ifVersionMatch("version").build();
            assertThatThrownBy(() -> op.stat(path, ifVersionMatch))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.Unsupported));

            final StatOptions ifVersionNotMatch =
                    StatOptions.builder().ifVersionNotMatch("version").build();
            assertThatThrownBy(() -> op.stat(path, ifVersionNotMatch))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.Unsupported));
        }
    }
}
