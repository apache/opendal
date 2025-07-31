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

package org.apache.opendal.test.behavior;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.apache.opendal.Capability;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.StatOptions;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AsyncStatOptionsTest extends BehaviorTestBase {

    @BeforeAll
    public void precondition() {
        Capability capability = asyncOp().info.fullCapability;
        assumeTrue(capability.read && capability.write && capability.stat);
    }

    @Test
    void testStatWithIfMatch() {
        assumeTrue(asyncOp().info.fullCapability.statWithIfMatch);

        String path = UUID.randomUUID().toString();
        byte[] content = generateBytes();
        asyncOp().write(path, content).join();
        Metadata meta = asyncOp().stat(path).join();
        StatOptions invalidOptions =
                StatOptions.builder().ifMatch("\"invalid\"").build();

        assertThatThrownBy(() -> asyncOp().stat(path, invalidOptions).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.ConditionNotMatch));

        StatOptions validOptions = StatOptions.builder().ifMatch(meta.getEtag()).build();
        Metadata result = asyncOp().stat(path, validOptions).join();
        assertThat(result).isNotNull();

        asyncOp().delete(path).join();
    }

    @Test
    void testStatWithIfNoneMatch() {
        assumeTrue(asyncOp().info.fullCapability.statWithIfNoneMatch);

        String path = UUID.randomUUID().toString();
        byte[] content = generateBytes();
        asyncOp().write(path, content).join();
        Metadata meta = asyncOp().stat(path).join();
        StatOptions matchingOptions =
                StatOptions.builder().ifNoneMatch(meta.getEtag()).build();

        assertThatThrownBy(() -> asyncOp().stat(path, matchingOptions).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.ConditionNotMatch));

        StatOptions nonMatchingOptions =
                StatOptions.builder().ifNoneMatch("\"invalid\"").build();
        Metadata result = asyncOp().stat(path, nonMatchingOptions).join();
        assertThat(result.getContentLength()).isEqualTo(meta.getContentLength());

        asyncOp().delete(path).join();
    }

    @Test
    void testStatWithIfModifiedSince() {
        assumeTrue(asyncOp().info.fullCapability.statWithIfModifiedSince);

        String path = UUID.randomUUID().toString();
        byte[] content = generateBytes();
        asyncOp().write(path, content).join();
        Metadata meta = asyncOp().stat(path).join();
        Instant since = meta.getLastModified().minus(1, ChronoUnit.SECONDS);
        StatOptions options1 = StatOptions.builder().ifModifiedSince(since).build();
        Metadata result1 = asyncOp().stat(path, options1).join();
        assertThat(result1.getLastModified()).isEqualTo(meta.getLastModified());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Instant futureTime = meta.getLastModified().plus(1, ChronoUnit.SECONDS);
        StatOptions options2 = StatOptions.builder().ifModifiedSince(futureTime).build();

        assertThatThrownBy(() -> asyncOp().stat(path, options2).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.ConditionNotMatch));

        asyncOp().delete(path).join();
    }

    @Test
    void testStatWithIfUnmodifiedSince() {
        assumeTrue(asyncOp().info.fullCapability.statWithIfUnmodifiedSince);

        String path = UUID.randomUUID().toString();
        byte[] content = generateBytes();
        asyncOp().write(path, content).join();
        Metadata meta = asyncOp().stat(path).join();
        Instant beforeTime = meta.getLastModified().minus(1, ChronoUnit.SECONDS);
        StatOptions options1 =
                StatOptions.builder().ifUnmodifiedSince(beforeTime).build();

        assertThatThrownBy(() -> asyncOp().stat(path, options1).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.ConditionNotMatch));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Instant afterTime = meta.getLastModified().plus(1, ChronoUnit.SECONDS);
        StatOptions options2 =
                StatOptions.builder().ifUnmodifiedSince(afterTime).build();
        Metadata result2 = asyncOp().stat(path, options2).join();

        assertThat(result2.getLastModified()).isEqualTo(meta.getLastModified());

        asyncOp().delete(path).join();
    }

    @Test
    void testStatWithVersion() {
        assumeTrue(asyncOp().info.fullCapability.statWithVersion);

        String path = UUID.randomUUID().toString();
        byte[] content = generateBytes();
        asyncOp().write(path, content).join();
        Metadata metadata = asyncOp().stat(path).join();
        String version = metadata.getVersion();

        assertThat(version).isNotNull();

        StatOptions versionOptions = StatOptions.builder().version(version).build();
        Metadata versionedMeta = asyncOp().stat(path, versionOptions).join();

        assertThat(versionedMeta.getVersion()).isEqualTo(version);
        asyncOp().write(path, content).join();
        Metadata metadata2 = asyncOp().stat(path).join();

        assertThat(metadata2.getVersion()).isNotEqualTo(version);
        Metadata oldVersionMetadata = asyncOp().stat(path, versionOptions).join();
        assertThat(oldVersionMetadata.getVersion()).isEqualTo(version);

        asyncOp().delete(path).join();
    }

    @Test
    void testStatWithNotExistingVersion() {
        assumeTrue(asyncOp().info.fullCapability.statWithVersion);

        String path = UUID.randomUUID().toString();
        byte[] content = generateBytes();
        asyncOp().write(path, content).join();
        String version = asyncOp().stat(path).join().getVersion();

        String path2 = UUID.randomUUID().toString();
        asyncOp().write(path2, content).join();

        StatOptions options = StatOptions.builder().version(version).build();

        assertThatThrownBy(() -> asyncOp().stat(path2, options).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));

        asyncOp().delete(path).join();
        asyncOp().delete(path2).join();
    }
}
