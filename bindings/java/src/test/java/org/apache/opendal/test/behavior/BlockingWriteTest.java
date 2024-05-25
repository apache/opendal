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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.util.UUID;
import org.apache.opendal.Capability;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BlockingWriteTest extends BehaviorTestBase {
    @BeforeAll
    public void precondition() {
        final Capability capability = op().info.fullCapability;
        assumeTrue(capability.read && capability.write && capability.blocking);
    }

    /**
     * Read not exist file should return NotFound.
     */
    @Test
    public void testBlockingReadNotExist() {
        final String path = UUID.randomUUID().toString();
        assertThatThrownBy(() -> op().read(path)).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
    }

    /**
     * Read full content should match.
     */
    @Test
    public void testBlockingReadFull() {
        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        op().write(path, content);
        final byte[] actualContent = op().read(path);
        assertThat(actualContent).isEqualTo(content);
        op().delete(path);
    }

    /**
     * Stat existing file should return metadata.
     */
    @Test
    public void testBlockingStatFile() {
        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        op().write(path, content);
        final Metadata meta = op().stat(path);
        assertThat(meta.isFile()).isTrue();
        assertThat(meta.getContentLength()).isEqualTo(content.length);

        op().delete(path);
    }
}
