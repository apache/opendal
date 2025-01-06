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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.util.Arrays;
import java.util.UUID;
import org.apache.opendal.Capability;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AsyncAppendTest extends BehaviorTestBase {
    @BeforeAll
    public void precondition() {
        final Capability capability = asyncOp().info.fullCapability;
        assumeTrue(capability.read && capability.write && capability.writeCanAppend);
    }

    @Test
    public void testAppendCreateAppend() {
        final String path = UUID.randomUUID().toString();
        final byte[] contentOne = generateBytes();
        final byte[] contentTwo = generateBytes();

        asyncOp().append(path, contentOne).join();
        asyncOp().append(path, contentTwo).join();

        final byte[] actualContent = asyncOp().read(path).join();
        assertThat(actualContent.length).isEqualTo(contentOne.length + contentTwo.length);
        assertThat(Arrays.copyOfRange(actualContent, 0, contentOne.length)).isEqualTo(contentOne);
        assertThat(Arrays.copyOfRange(actualContent, contentOne.length, actualContent.length))
                .isEqualTo(contentTwo);

        asyncOp().delete(path).join();
    }
}
