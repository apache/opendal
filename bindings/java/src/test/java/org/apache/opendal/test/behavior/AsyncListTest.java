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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.opendal.Capability;
import org.apache.opendal.Entry;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AsyncListTest extends BehaviorTestBase {

    @BeforeAll
    public void precondition() {
        final Capability capability = asyncOp().info.fullCapability;
        assumeTrue(capability.read && capability.write && capability.list && capability.createDir);
    }

    /**
     * List dir should return newly created file.
     */
    @Test
    public void testListDir() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        asyncOp().write(path, content).join();

        final List<Entry> entries = asyncOp().list(parent + "/").join();
        boolean found = false;
        for (Entry entry : entries) {
            if (entry.getPath().equals(path)) {
                Metadata meta = asyncOp().stat(path).join();
                assertTrue(meta.isFile());
                assertThat(meta.getContentLength()).isEqualTo(content.length);

                found = true;
            }
        }
        assertTrue(found);
        asyncOp().delete(path).join();
    }

    /**
     * listing a directory, which contains more objects than a single page can take.
     */
    @Test
    public void testListRichDir() {
        final String parent = "test_list_rich_dir";
        asyncOp().createDir(parent + "/").join();
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expected.add(String.format("%s/file-%d", parent, i));
        }

        for (String path : expected) {
            asyncOp().write(path, parent).join();
        }

        final List<Entry> entries = asyncOp().list(parent + "/").join();
        final List<String> actual =
                entries.stream().map(Entry::getPath).sorted().collect(Collectors.toList());

        Collections.sort(expected);
        assertThat(actual).isEqualTo(expected);
        asyncOp().removeAll(parent + "/").join();
    }

    /**
     * List empty dir should return nothing.
     */
    @Test
    public void testListEmptyDir() {
        final String dir = String.format("%s/", UUID.randomUUID());
        asyncOp().createDir(dir).join();

        final List<Entry> entries = asyncOp().list(dir).join();
        assertThat(entries).isEmpty();

        asyncOp().delete(dir).join();
    }

    /**
     * List non exist dir should return nothing.
     */
    @Test
    public void testListNotExistDir() {
        final String dir = String.format("%s/", UUID.randomUUID());

        final List<Entry> entries = asyncOp().list(dir).join();
        assertThat(entries).isEmpty();
    }

    /**
     * List dir should return correct sub dir.
     */
    @Test
    public void testListSubDir() {
        final String path = String.format("%s/", UUID.randomUUID());
        asyncOp().createDir(path).join();

        final List<Entry> entries = asyncOp().list("/").join();
        boolean found = false;
        for (Entry entry : entries) {
            if (entry.getPath().equals(path)) {
                Metadata metadata = entry.getMetadata();
                assertTrue(metadata.isDir());
                found = true;
            }
        }
        assertTrue(found);

        asyncOp().delete(path).join();
    }

    /**
     * List dir should also to list nested dir.
     */
    @Test
    public void testListNestedDir() {
        final String dir = String.format("%s/%s/", UUID.randomUUID(), UUID.randomUUID());
        final String fileName = UUID.randomUUID().toString();
        final String filePath = String.format("%s/%s", dir, fileName);
        final String dirName = String.format("%s/", UUID.randomUUID());
        final String dirPath = String.format("%s/%s", dir, dirName);
        final String content = "test_list_nested_dir";

        asyncOp().createDir(dir).join();
        asyncOp().write(filePath, content).join();
        asyncOp().createDir(dirPath).join();

        final List<Entry> entries = asyncOp().list(dir).join();
        assertThat(entries).hasSize(2);

        for (Entry entry : entries) {
            // check file
            if (entry.getPath().equals(filePath)) {
                Metadata metadata = entry.getMetadata();
                assertTrue(metadata.isFile());
                assertThat(metadata.getContentLength()).isEqualTo(content.length());
                // check dir
            } else if (entry.getPath().equals(dirPath)) {
                Metadata metadata = entry.getMetadata();
                assertTrue(metadata.isDir());
            }
        }

        asyncOp().removeAll(dir).join();
    }

    /**
     * Remove all should remove all in this path.
     */
    @Test
    public void testRemoveAll() {
        final String parent = UUID.randomUUID().toString();
        final String[] expected = new String[] {
            "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
        };
        for (String path : expected) {
            if (path.endsWith("/")) {
                asyncOp().createDir(String.format("%s/%s", parent, path)).join();
            } else {
                asyncOp()
                        .write(String.format("%s/%s", parent, path), "test_scan")
                        .join();
            }
        }

        asyncOp().removeAll(parent + "/x/").join();

        for (String path : expected) {
            if (path.endsWith("/")) {
                continue;
            }
            assertThatThrownBy(() ->
                            asyncOp().stat(String.format("%s/%s", parent, path)).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        asyncOp().removeAll(parent + "/").join();
    }
}
