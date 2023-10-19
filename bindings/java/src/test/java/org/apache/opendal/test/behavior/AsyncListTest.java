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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.opendal.Capability;
import org.apache.opendal.Entry;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.args.OpListArgs;
import org.apache.opendal.args.OpListArgs.Metakey;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AsyncListTest extends BehaviorTestBase {

    @BeforeAll
    public void precondition() {
        final Capability capability = op().info.fullCapability;
        assumeTrue(capability.read && capability.write && capability.list);
    }

    /**
     * List dir should return newly created file.
     */
    @Test
    public void testListDir() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        op().write(path, content).join();

        final List<Entry> entries = op().list(parent + "/").join();
        boolean found = false;
        for (Entry entry : entries) {
            if (entry.getPath().equals(path)) {
                Metadata meta = op().stat(path).join();
                assertTrue(meta.isFile());
                assertThat(meta.getContentLength()).isEqualTo(content.length);

                found = true;
            }
        }
        assertTrue(found);
        op().delete(path).join();
    }

    /**
     * List dir with metakey
     */
    @Test
    public void testListDirWithMetakey() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        op().write(path, content).join();

        final OpListArgs args = new OpListArgs(parent + "/");
        args.setMetakeys(
                Metakey.Mode,
                Metakey.CacheControl,
                Metakey.ContentDisposition,
                Metakey.ContentLength,
                Metakey.ContentMd5,
                Metakey.ContentType,
                Metakey.Etag,
                Metakey.Version,
                Metakey.LastModified);
        final List<Entry> entries = op().list(args).join();
        boolean found = false;
        for (Entry entry : entries) {
            if (entry.getPath().equals(path)) {
                Metadata metadata = entry.getMetadata();
                assertTrue(metadata.isFile());
                assertThat(metadata.getContentLength()).isEqualTo(content.length);

                metadata.getCacheControl();
                metadata.getContentDisposition();
                metadata.getContentMd5();
                metadata.getContentType();
                metadata.getEtag();
                metadata.getVersion();
                metadata.getLastModified();
                found = true;
            }
        }
        assertTrue(found);
        op().delete(path).join();
    }

    /**
     * List dir with metakey complete
     */
    @Test
    public void testListDirWithMetakeyComplete() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        op().write(path, content).join();

        final OpListArgs args = new OpListArgs(parent + "/");
        args.setMetakeys(Metakey.Complete);
        final List<Entry> entries = op().list(args).join();
        boolean found = false;
        for (Entry entry : entries) {
            if (entry.getPath().equals(path)) {
                Metadata metadata = entry.getMetadata();
                assertTrue(metadata.isFile());
                assertThat(metadata.getContentLength()).isEqualTo(content.length);

                metadata.getCacheControl();
                metadata.getContentDisposition();
                metadata.getContentMd5();
                metadata.getContentType();
                metadata.getEtag();
                metadata.getVersion();
                metadata.getLastModified();
                found = true;
            }
        }
        assertTrue(found);
        op().delete(path).join();
    }

    /**
     * listing a directory, which contains more objects than a single page can take.
     */
    @Test
    public void testListRichDir() {
        final String parent = "test_list_rich_dir";
        op().createDir(parent + "/").join();
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            expected.add(String.format("%s/file-%d", parent, i));
        }

        for (String path : expected) {
            op().write(path, parent).join();
        }

        final List<Entry> entries = op().list(parent + "/").join();
        final List<String> actual =
                entries.stream().map(Entry::getPath).sorted().collect(Collectors.toList());

        Collections.sort(expected);
        assertThat(actual).isEqualTo(expected);
        op().removeAll(parent + "/").join();
    }

    /**
     * List empty dir should return nothing.
     */
    @Test
    public void testListEmptyDir() {
        final String dir = String.format("%s/", UUID.randomUUID());
        op().createDir(dir).join();

        final List<Entry> entries = op().list(dir).join();
        assertThat(entries).isEmpty();

        op().delete(dir).join();
    }

    /**
     * List non exist dir should return nothing.
     */
    @Test
    public void testListNotExistDir() {
        final String dir = String.format("%s/", UUID.randomUUID());

        final List<Entry> entries = op().list(dir).join();
        assertThat(entries).isEmpty();
    }

    /**
     * List dir should return correct sub dir.
     */
    @Test
    public void testListSubDir() {
        final String path = String.format("%s/", UUID.randomUUID());
        op().createDir(path).join();

        final List<Entry> entries = op().list("/").join();
        boolean found = false;
        for (Entry entry : entries) {
            if (entry.getPath().equals(path)) {
                Metadata metadata = entry.getMetadata();
                assertTrue(metadata.isDir());
                found = true;
            }
        }
        assertTrue(found);

        op().delete(path).join();
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

        op().createDir(dir).join();
        op().write(filePath, content).join();
        op().createDir(dirPath).join();

        final List<Entry> entries = op().list(dir).join();
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

        op().removeAll(dir).join();
    }

    /**
     * List with start after should start listing after the specified key
     */
    @Test
    public void testListWithStartAfter() {
        if (!op().info.fullCapability.listWithStartAfter) {
            return;
        }
        final String dir = UUID.randomUUID().toString() + "/";
        op().createDir(dir).join();

        final String[] given = new String[] {
            String.format("%sfile-0", dir),
            String.format("%sfile-1", dir),
            String.format("%sfile-2", dir),
            String.format("%sfile-3", dir),
            String.format("%sfile-4", dir),
            String.format("%sfile-5", dir),
        };

        for (String path : given) {
            op().write(path, "content").join();
        }

        final OpListArgs args = new OpListArgs(dir);
        args.setStartAfter(given[2]);
        final List<Entry> entries = op().list(args).join();
        final List<String> expected = entries.stream().map(Entry::getPath).collect(Collectors.toList());

        Assertions.assertThat(expected).isEqualTo(Arrays.asList(given[3], given[4], given[5]));

        op().removeAll(dir).join();
    }

    @Test
    public void testScanRoot() {
        final OpListArgs args = new OpListArgs("");
        args.setDelimiter("");

        final List<Entry> entries = op().list(args).join();
        final Set<String> actual = entries.stream().map(Entry::getPath).collect(Collectors.toSet());

        assertTrue(!actual.contains("/"), "empty root shouldn't return itself");
        assertTrue(!actual.contains(""), "empty root shouldn't return empty");
    }

    /**
     * Walk top down should output as expected
     */
    @Test
    public void testScan() {
        final String parent = UUID.randomUUID().toString();
        final String[] expected = new String[] {
            "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
        };
        for (String path : expected) {
            if (path.endsWith("/")) {
                op().createDir(String.format("%s/%s", parent, path)).join();
            } else {
                op().write(String.format("%s/%s", parent, path), "test_scan").join();
            }
        }
        final OpListArgs args = new OpListArgs(parent + "/x/");
        args.setDelimiter("");
        final List<Entry> entries = op().list(args).join();
        final Set<String> actual = entries.stream().map(Entry::getPath).collect(Collectors.toSet());

        assertThat(actual).contains(parent + "/x/y", parent + "/x/x/y", parent + "/x/x/x/y");

        op().removeAll(parent + "/").join();
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
                op().createDir(String.format("%s/%s", parent, path)).join();
            } else {
                op().write(String.format("%s/%s", parent, path), "test_scan").join();
            }
        }

        op().removeAll(parent + "/x/").join();

        for (String path : expected) {
            if (path.endsWith("/")) {
                continue;
            }
            assertThatThrownBy(() ->
                            op().stat(String.format("%s/%s", parent, path)).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        op().removeAll(parent + "/").join();
    }
}
