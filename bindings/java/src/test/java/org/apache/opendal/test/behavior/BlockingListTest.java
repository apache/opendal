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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.util.List;
import java.util.UUID;
import org.apache.opendal.Capability;
import org.apache.opendal.Entry;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BlockingListTest extends BehaviorTestBase {
    @BeforeAll
    public void precondition() {
        final Capability capability = op().info.fullCapability;
        assumeTrue(capability.read
                && capability.write
                && capability.copy
                && capability.blocking
                && capability.list
                && capability.createDir);
    }

    @Test
    public void testBlockingListDir() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        op().write(path, content);

        final List<Entry> list = op().list(parent + "/");
        boolean found = false;
        for (Entry entry : list) {
            if (entry.getPath().equals(path)) {
                Metadata meta = op().stat(path);
                assertTrue(meta.isFile());
                assertThat(meta.getContentLength()).isEqualTo(content.length);

                found = true;
            }
        }
        assertTrue(found);

        op().delete(path);
    }

    @Test
    public void testBlockingListNonExistDir() {
        final String dir = String.format("%s/", UUID.randomUUID());

        final List<Entry> list = op().list(dir);
        assertTrue(list.isEmpty());
    }

    /**
     * Remove all should remove all in this path.
     */
    @Test
    public void testBlockingRemoveAll() {
        final String parent = UUID.randomUUID().toString();
        final String[] expected = new String[] {
            "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
        };
        for (String path : expected) {
            if (path.endsWith("/")) {
                op().createDir(String.format("%s/%s", parent, path));
            } else {
                op().write(String.format("%s/%s", parent, path), "test_scan");
            }
        }

        op().removeAll(parent + "/x/");

        for (String path : expected) {
            if (path.endsWith("/")) {
                continue;
            }
            assertThatThrownBy(() -> op().stat(String.format("%s/%s", parent, path)))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
        }

        op().removeAll(parent + "/");
    }
}
