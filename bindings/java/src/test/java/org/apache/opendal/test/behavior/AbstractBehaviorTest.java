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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.opendal.BlockingOperator;
import org.apache.opendal.Capability;
import org.apache.opendal.Entry;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.args.OpList.Metakey;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractBehaviorTest {
    protected final String scheme;
    protected final Map<String, String> config;
    protected Operator operator;
    protected BlockingOperator blockingOperator;

    protected AbstractBehaviorTest(String scheme) {
        this(scheme, createSchemeConfig(scheme));
    }

    protected AbstractBehaviorTest(String scheme, Map<String, String> config) {
        this.scheme = scheme;
        this.config = config;
    }

    @BeforeAll
    public void setup() {
        assertThat(isSchemeEnabled(config))
                .describedAs("service test for " + scheme + " is not enabled.")
                .isTrue();
        this.operator = Operator.of(scheme, config);
        this.blockingOperator = BlockingOperator.of(scheme, config);
    }

    @AfterAll
    public void teardown() {
        if (operator != null) {
            operator.close();
            operator = null;
        }
        if (blockingOperator != null) {
            blockingOperator.close();
            blockingOperator = null;
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncWriteTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.read && capability.write);
        }

        /**
         * Read not exist file should return NotFound.
         */
        @Test
        public void testReadNotExist() {
            final String path = UUID.randomUUID().toString();
            assertThatThrownBy(() -> operator.read(path).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        /**
         * Read full content should match.
         */
        @Test
        public void testReadFull() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            operator.write(path, content).join();
            final byte[] actualContent = operator.read(path).join();
            assertThat(actualContent).isEqualTo(content);
            operator.delete(path).join();
        }

        /**
         * Stat not exist file should return NotFound.
         */
        @Test
        public void testStatNotExist() {
            final String path = UUID.randomUUID().toString();
            assertThatThrownBy(() -> operator.stat(path).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        /**
         * Stat existing file should return metadata.
         */
        @Test
        public void testStatFile() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            operator.write(path, content).join();
            final Metadata meta = operator.stat(path).join();
            assertThat(meta.isFile()).isTrue();
            assertThat(meta.getContentLength()).isEqualTo(content.length);

            operator.delete(path).join();
        }

        /**
         * Write file with non ascii name should succeed.
         */
        @Test
        public void testWriteFileWithNonAsciiName() {
            final String path = "❌😱中文.test";
            final byte[] content = generateBytes();
            operator.write(path, content).join();
            final Metadata meta = operator.stat(path).join();
            assertThat(meta.isFile()).isTrue();
            assertThat(meta.getContentLength()).isEqualTo(content.length);

            operator.delete(path).join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncAppendTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.writeCanAppend);
        }

        @Test
        public void testAppendCreateAppend() {
            final String path = UUID.randomUUID().toString();
            final byte[] contentOne = generateBytes();
            final byte[] contentTwo = generateBytes();

            operator.append(path, contentOne).join();
            operator.append(path, contentTwo).join();

            final byte[] actualContent = operator.read(path).join();
            assertThat(actualContent.length).isEqualTo(contentOne.length + contentTwo.length);
            assertThat(Arrays.copyOfRange(actualContent, 0, contentOne.length)).isEqualTo(contentOne);
            assertThat(Arrays.copyOfRange(actualContent, contentOne.length, actualContent.length))
                    .isEqualTo(contentTwo);

            operator.delete(path).join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncCreateDirTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.createDir);
        }

        /**
         * Create dir with dir path should succeed.
         */
        @Test
        public void testCreateDir() {
            final String path = String.format("%s/", UUID.randomUUID().toString());
            operator.createDir(path).join();

            final Metadata meta = operator.stat(path).join();
            assertThat(meta.isFile()).isFalse();

            operator.delete(path).join();
        }

        /**
         * Create dir on existing dir should succeed.
         */
        @Test
        public void testCreateDirExisting() {
            final String path = String.format("%s/", UUID.randomUUID().toString());
            operator.createDir(path).join();
            operator.createDir(path).join();

            final Metadata meta = operator.stat(path).join();
            assertThat(meta.isFile()).isFalse();

            operator.delete(path).join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncCopyTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.copy);
        }

        /**
         * Copy a file with ascii name and test contents.
         */
        @Test
        public void testCopyFileWithAsciiName() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            operator.write(sourcePath, sourceContent).join();

            final String targetPath = UUID.randomUUID().toString();

            operator.copy(sourcePath, targetPath).join();

            assertThat(operator.read(targetPath).join()).isEqualTo(sourceContent);

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }

        /**
         * Copy a file with non ascii name and test contents.
         */
        @Test
        public void testCopyFileWithNonAsciiName() {
            final String sourcePath = "🐂🍺中文.docx";
            final String targetPath = "😈🐅Français.docx";
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();
            operator.copy(sourcePath, targetPath).join();

            assertThat(operator.read(targetPath).join()).isEqualTo(content);

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }

        /**
         * Copy a nonexistent source should return an error.
         */
        @Test
        public void testCopyNonExistingSource() {
            final String sourcePath = UUID.randomUUID().toString();
            final String targetPath = UUID.randomUUID().toString();

            assertThatThrownBy(() -> operator.copy(sourcePath, targetPath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        /**
         * Copy a dir as source should return an error.
         */
        @Test
        public void testCopySourceDir() {
            final String sourcePath = String.format("%s/", UUID.randomUUID().toString());
            final String targetPath = UUID.randomUUID().toString();

            assertThatThrownBy(() -> operator.copy(sourcePath, targetPath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.IsADirectory));
        }

        /**
         * Copy to a dir should return an error.
         */
        @Test
        public void testCopyTargetDir() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();

            final String targetPath = String.format("%s/", UUID.randomUUID().toString());
            operator.createDir(targetPath).join();

            assertThatThrownBy(() -> operator.copy(sourcePath, targetPath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.IsADirectory));

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }

        /**
         * Copy a file to self should return an error.
         */
        @Test
        public void testCopySelf() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();

            assertThatThrownBy(() -> operator.copy(sourcePath, sourcePath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.IsSameFile));

            operator.delete(sourcePath).join();
        }

        /**
         * Copy to a nested path, parent path should be created successfully.
         */
        @Test
        public void testCopyNested() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();

            final String targetPath = String.format(
                    "%s/%s/%s",
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString());

            operator.copy(sourcePath, targetPath).join();

            assertThat(operator.read(targetPath).join()).isEqualTo(content);

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }

        /**
         * Copy to a exist path should overwrite successfully.
         */
        @Test
        public void testCopyOverwrite() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            operator.write(sourcePath, sourceContent).join();

            final String targetPath = UUID.randomUUID().toString();
            final byte[] targetContent = generateBytes();
            assertNotEquals(sourceContent, targetContent);

            operator.write(targetPath, targetContent).join();

            operator.copy(sourcePath, targetPath).join();

            assertThat(operator.read(targetPath).join()).isEqualTo(sourceContent);

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncRenameTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.rename);
        }

        /**
         * Rename a file and test with stat.
         */
        @Test
        public void testRenameFile() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();

            final String targetPath = UUID.randomUUID().toString();

            operator.rename(sourcePath, targetPath).join();

            assertThatThrownBy(() -> operator.stat(sourcePath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));

            assertThat(operator.read(targetPath).join()).isEqualTo(content);

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }

        /**
         * Rename a nonexistent source should return an error.
         */
        @Test
        public void testRenameNonExistingSource() {
            final String sourcePath = UUID.randomUUID().toString();
            final String targetPath = UUID.randomUUID().toString();

            assertThatThrownBy(() -> operator.rename(sourcePath, targetPath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
        }

        /**
         * Rename a dir as source should return an error.
         */
        @Test
        public void testRenameSourceDir() {
            final String sourcePath = String.format("%s/", UUID.randomUUID().toString());
            final String targetPath = UUID.randomUUID().toString();

            operator.createDir(sourcePath).join();

            assertThatThrownBy(() -> operator.rename(sourcePath, targetPath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.IsADirectory));

            operator.delete(sourcePath).join();
        }

        /**
         * Rename to a dir should return an error.
         */
        @Test
        public void testRenameTargetDir() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();

            final String targetPath = String.format("%s/", UUID.randomUUID().toString());

            operator.createDir(targetPath).join();

            assertThatThrownBy(() -> operator.rename(sourcePath, targetPath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.IsADirectory));

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }

        /**
         * Rename a file to self should return an error.
         */
        @Test
        public void testRenameSelf() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();

            assertThatThrownBy(() -> operator.rename(sourcePath, sourcePath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.IsSameFile));

            operator.delete(sourcePath).join();
        }

        /**
         * Rename to a nested path, parent path should be created successfully.
         */
        @Test
        public void testRenameNested() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            operator.write(sourcePath, content).join();

            final String targetPath = String.format(
                    "%s/%s/%s",
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString());

            operator.rename(sourcePath, targetPath).join();

            assertThatThrownBy(() -> operator.stat(sourcePath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));

            assertThat(operator.read(targetPath).join()).isEqualTo(content);

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }

        /**
         * Rename to a exist path should overwrite successfully.
         */
        @Test
        public void testRenameOverwrite() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            operator.write(sourcePath, sourceContent).join();

            final String targetPath = UUID.randomUUID().toString();
            final byte[] targetContent = generateBytes();
            assertNotEquals(sourceContent, targetContent);

            operator.write(targetPath, targetContent).join();

            operator.rename(sourcePath, targetPath).join();

            assertThatThrownBy(() -> operator.stat(sourcePath).join())
                    .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));

            assertThat(operator.read(targetPath).join()).isEqualTo(sourceContent);

            operator.delete(sourcePath).join();
            operator.delete(targetPath).join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class AsyncListTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = operator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.list);
        }

        /**
         * List dir should return newly created file.
         */
        @Test
        public void testListDir() {
            final String parent = UUID.randomUUID().toString();
            final String path = String.format("%s/%s", parent, UUID.randomUUID().toString());
            final byte[] content = generateBytes();

            operator.write(path, content).join();

            final List<Entry> entries = operator.list(parent + "/").join();
            boolean found = false;
            for (Entry entry : entries) {
                if (entry.getPath().equals(path)) {
                    Metadata meta = operator.stat(path).join();
                    assertTrue(meta.isFile());
                    assertThat(meta.getContentLength()).isEqualTo(content.length);

                    found = true;
                }
            }
            assertTrue(found);
            operator.delete(path).join();
        }

        /**
         * List dir with metakey
         */
        @Test
        public void testListDirWithMetakey() {
            final String parent = UUID.randomUUID().toString();
            final String path = String.format("%s/%s", parent, UUID.randomUUID().toString());
            final byte[] content = generateBytes();

            operator.write(path, content).join();

            final List<Entry> entries = operator.listWith(parent + "/")
                    .metakeys(
                            Metakey.Mode,
                            Metakey.CacheControl,
                            Metakey.ContentDisposition,
                            Metakey.ContentLength,
                            Metakey.ContentMd5,
                            Metakey.ContentType,
                            Metakey.Etag,
                            Metakey.Version,
                            Metakey.LastModified)
                    .build()
                    .call()
                    .join();
            boolean found = false;
            for (Entry entry : entries) {
                if (entry.getPath().equals(path)) {
                    Metadata metadata = entry.getMetadata();
                    assertTrue(metadata.isFile());
                    assertThat(metadata.getContentLength()).isEqualTo(content.length);
                    // We don't care about the value, we just to check there is no panic.
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
            operator.delete(path).join();
        }

        /**
         * List dir with metakey complete
         */
        @Test
        public void testListDirWithMetakeyComplete() {
            final String parent = UUID.randomUUID().toString();
            final String path = String.format("%s/%s", parent, UUID.randomUUID().toString());
            final byte[] content = generateBytes();

            operator.write(path, content).join();

            final List<Entry> entries = operator.listWith(parent + "/")
                    .metakeys(Metakey.Complete)
                    .build()
                    .call()
                    .join();
            boolean found = false;
            for (Entry entry : entries) {
                if (entry.getPath().equals(path)) {
                    Metadata metadata = entry.getMetadata();
                    assertTrue(metadata.isFile());
                    assertThat(metadata.getContentLength()).isEqualTo(content.length);
                    // We don't care about the value, we just to check there is no panic.
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
            operator.delete(path).join();
        }

        /**
         * listing a directory, which contains more objects than a single page can take.
         */
        @Test
        public void testListRichDir() {
            final String parent = "test_list_rich_dir";
            operator.createDir(parent + "/").join();
            final List<String> expected = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                expected.add(String.format("%s/file-%d", parent, i));
            }

            for (String path : expected) {
                operator.write(path, parent).join();
            }

            final List<Entry> entries = operator.list(parent + "/").join();
            final List<String> actual =
                    entries.stream().map(Entry::getPath).sorted().collect(Collectors.toList());

            Collections.sort(expected);
            assertThat(actual).isEqualTo(expected);
            operator.removeAll(parent + "/").join();
        }

        /**
         * List empty dir should return nothing.
         */
        @Test
        public void testListEmptyDir() {
            final String dir = String.format("%s/", UUID.randomUUID().toString());
            operator.createDir(dir).join();

            final List<Entry> entries = operator.list(dir).join();
            assertThat(entries).isEmpty();

            operator.delete(dir).join();
        }

        /**
         * List non exist dir should return nothing.
         */
        @Test
        public void testListNotExistDir() {
            final String dir = String.format("%s/", UUID.randomUUID().toString());

            final List<Entry> entries = operator.list(dir).join();
            assertThat(entries).isEmpty();
        }

        /**
         * List dir should return correct sub dir.
         */
        @Test
        public void testListSubDir() {
            final String path = String.format("%s/", UUID.randomUUID().toString());
            operator.createDir(path).join();

            final List<Entry> entries = operator.list("/").join();
            boolean found = false;
            for (Entry entry : entries) {
                if (entry.getPath().equals(path)) {
                    Metadata metadata = entry.getMetadata();
                    assertTrue(metadata.isDir());
                    found = true;
                }
            }
            assertTrue(found);

            operator.delete(path).join();
        }

        /**
         * List dir should also to list nested dir.
         */
        @Test
        public void testListNestedDir() {
            final String dir = String.format(
                    "%s/%s/", UUID.randomUUID().toString(), UUID.randomUUID().toString());
            final String fileName = UUID.randomUUID().toString();
            final String filePath = String.format("%s/%s", dir, fileName);
            final String dirName = String.format("%s/", UUID.randomUUID().toString());
            final String dirPath = String.format("%s/%s", dir, dirName);
            final String content = "test_list_nested_dir";

            operator.createDir(dir).join();
            operator.write(filePath, content).join();
            operator.createDir(dirPath).join();

            final List<Entry> entries = operator.list(dir).join();
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

            operator.removeAll(dir).join();
        }

        /**
         * List with start after should start listing after the specified key
         */
        @Test
        public void testListWithStartAfter() {
            if (!operator.info.fullCapability.listWithStartAfter) {
                return;
            }
            final String dir = String.format("%s/", UUID.randomUUID().toString());
            operator.createDir(dir).join();

            final String[] given = new String[] {
                String.format("%sfile-0", dir),
                String.format("%sfile-1", dir),
                String.format("%sfile-2", dir),
                String.format("%sfile-3", dir),
                String.format("%sfile-4", dir),
                String.format("%sfile-5", dir),
            };

            for (String path : given) {
                operator.write(path, "content").join();
            }

            final List<Entry> entries =
                    operator.listWith(dir).startAfter(given[2]).build().call().join();
            final List<String> expected = entries.stream().map(Entry::getPath).collect(Collectors.toList());

            assertThat(expected).isEqualTo(Arrays.asList(given[3], given[4], given[5]));

            operator.removeAll(dir).join();
        }

        @Test
        public void testScanRoot() {
            final List<Entry> entries =
                    operator.listWith("").delimiter("").build().call().join();
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
                    operator.createDir(String.format("%s/%s", parent, path)).join();
                } else {
                    operator.write(String.format("%s/%s", parent, path), "test_scan")
                            .join();
                }
            }
            final List<Entry> entries = operator.listWith(String.format("%s/x/", parent))
                    .delimiter("")
                    .build()
                    .call()
                    .join();
            final Set<String> actual = entries.stream().map(Entry::getPath).collect(Collectors.toSet());

            assertThat(actual).contains(parent + "/x/y", parent + "/x/x/y", parent + "/x/x/x/y");

            operator.removeAll(parent + "/").join();
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
                    operator.createDir(String.format("%s/%s", parent, path)).join();
                } else {
                    operator.write(String.format("%s/%s", parent, path), "test_scan")
                            .join();
                }
            }

            operator.removeAll(parent + "/x/").join();

            for (String path : expected) {
                if (path.endsWith("/")) {
                    continue;
                }
                assertThatThrownBy(() -> operator.stat(String.format("%s/%s", parent, path))
                                .join())
                        .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
            }

            operator.removeAll(parent + "/").join();
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class BlockingWriteTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = blockingOperator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.blocking);
        }

        /**
         * Read not exist file should return NotFound.
         */
        @Test
        public void testBlockingReadNotExist() {
            final String path = UUID.randomUUID().toString();
            assertThatThrownBy(() -> blockingOperator.read(path))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
        }

        /**
         * Read full content should match.
         */
        @Test
        public void testBlockingReadFull() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            blockingOperator.write(path, content);
            final byte[] actualContent = blockingOperator.read(path);
            assertThat(actualContent).isEqualTo(content);
            blockingOperator.delete(path);
        }

        /**
         * Stat existing file should return metadata.
         */
        @Test
        public void testBlockingStatFile() {
            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            blockingOperator.write(path, content);
            final Metadata meta = blockingOperator.stat(path);
            assertThat(meta.isFile()).isTrue();
            assertThat(meta.getContentLength()).isEqualTo(content.length);

            blockingOperator.delete(path);
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class BlockingCreateDirTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = blockingOperator.info.fullCapability;
            assumeTrue(capability.createDir);
        }

        /**
         * Create dir with dir path should succeed.
         */
        @Test
        public void testBlockingCreateDir() {
            final String path = String.format("%s/", UUID.randomUUID().toString());
            blockingOperator.createDir(path);

            final Metadata meta = blockingOperator.stat(path);
            assertThat(meta.isFile()).isFalse();

            blockingOperator.delete(path);
        }

        /**
         * Create dir on existing dir should succeed.
         */
        @Test
        public void testBlockingDirExisting() {
            final String path = String.format("%s/", UUID.randomUUID().toString());
            blockingOperator.createDir(path);
            blockingOperator.createDir(path);

            final Metadata meta = blockingOperator.stat(path);
            assertThat(meta.isFile()).isFalse();

            blockingOperator.delete(path);
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class BlockingCopyTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = blockingOperator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.copy);
        }

        /**
         * Copy a file and test with stat.
         */
        @Test
        public void testBlockingCopyFile() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            final String targetPath = UUID.randomUUID().toString();

            blockingOperator.copy(sourcePath, targetPath);

            assertThat(blockingOperator.read(targetPath)).isEqualTo(sourceContent);

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }

        /**
         * Copy a nonexistent source should return an error.
         */
        @Test
        public void testBlockingCopyNonExistingSource() {
            final String sourcePath = UUID.randomUUID().toString();
            final String targetPath = UUID.randomUUID().toString();

            assertThatThrownBy(() -> blockingOperator.copy(sourcePath, targetPath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
        }

        /**
         * Copy a dir as source should return an error.
         */
        @Test
        public void testBlockingCopySourceDir() {
            final String sourcePath = String.format("%s/", UUID.randomUUID().toString());
            final String targetPath = UUID.randomUUID().toString();

            blockingOperator.createDir(sourcePath);

            assertThatThrownBy(() -> blockingOperator.copy(sourcePath, targetPath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.IsADirectory));

            blockingOperator.delete(sourcePath);
        }

        /**
         * Copy to a dir should return an error.
         */
        @Test
        public void testBlockingCopyTargetDir() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            final String targetPath = String.format("%s/", UUID.randomUUID().toString());

            blockingOperator.createDir(targetPath);

            assertThatThrownBy(() -> blockingOperator.copy(sourcePath, targetPath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.IsADirectory));

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }

        /**
         * Copy a file to self should return an error.
         */
        @Test
        public void testBlockingCopySelf() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            assertThatThrownBy(() -> blockingOperator.copy(sourcePath, sourcePath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.IsSameFile));

            blockingOperator.delete(sourcePath);
        }

        /**
         * Copy to a nested path, parent path should be created successfully.
         */
        @Test
        public void testBlockingCopyNested() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] content = generateBytes();

            blockingOperator.write(sourcePath, content);

            final String targetPath = String.format(
                    "%s/%s/%s",
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString());

            blockingOperator.copy(sourcePath, targetPath);

            assertThat(blockingOperator.read(targetPath)).isEqualTo(content);

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }

        /**
         * Copy to a exist path should overwrite successfully.
         */
        @Test
        public void testBlockingCopyOverwrite() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            final String targetPath = UUID.randomUUID().toString();
            final byte[] targetContent = generateBytes();
            assertNotEquals(sourceContent, targetContent);

            blockingOperator.write(targetPath, targetContent);

            blockingOperator.copy(sourcePath, targetPath);

            assertThat(blockingOperator.read(targetPath)).isEqualTo(sourceContent);

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class BlockingRenameTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = blockingOperator.info.fullCapability;
            assumeTrue(capability.read && capability.write && capability.blocking && capability.rename);
        }

        /**
         * Rename a file and test with stat.
         */
        @Test
        public void testBlockingRenameFile() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            final String targetPath = UUID.randomUUID().toString();

            blockingOperator.rename(sourcePath, targetPath);

            assertThatThrownBy(() -> blockingOperator.stat(sourcePath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));

            assertThat(blockingOperator.stat(targetPath).getContentLength()).isEqualTo(sourceContent.length);

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }

        /**
         * Rename a nonexistent source should return an error.
         */
        @Test
        public void testBlockingRenameNonExistingSource() {
            final String sourcePath = UUID.randomUUID().toString();
            final String targetPath = UUID.randomUUID().toString();

            assertThatThrownBy(() -> blockingOperator.rename(sourcePath, targetPath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
        }

        /**
         * Rename a dir as source should return an error.
         */
        @Test
        public void testBlockingRenameSourceDir() {
            final String sourcePath = String.format("%s/", UUID.randomUUID().toString());
            final String targetPath = UUID.randomUUID().toString();

            blockingOperator.createDir(sourcePath);

            assertThatThrownBy(() -> blockingOperator.rename(sourcePath, targetPath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.IsADirectory));
        }

        /**
         * Rename to a dir should return an error.
         */
        @Test
        public void testBlockingRenameTargetDir() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            final String targetPath = String.format("%s/", UUID.randomUUID().toString());

            blockingOperator.createDir(targetPath);

            assertThatThrownBy(() -> blockingOperator.rename(sourcePath, targetPath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.IsADirectory));

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }

        /**
         * Rename a file to self should return an error.
         */
        @Test
        public void testBlockingRenameSelf() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            assertThatThrownBy(() -> blockingOperator.rename(sourcePath, sourcePath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.IsSameFile));

            blockingOperator.delete(sourcePath);
        }

        /**
         * Rename to a nested path, parent path should be created successfully.
         */
        @Test
        public void testBlockingRenameNested() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            final String targetPath = String.format(
                    "%s/%s/%s",
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString());

            blockingOperator.rename(sourcePath, targetPath);

            assertThatThrownBy(() -> blockingOperator.stat(sourcePath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));

            assertThat(blockingOperator.read(targetPath)).isEqualTo(sourceContent);

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }

        /**
         * Rename to a exist path should overwrite successfully.
         */
        @Test
        public void testBlockingRenameOverwrite() {
            final String sourcePath = UUID.randomUUID().toString();
            final byte[] sourceContent = generateBytes();

            blockingOperator.write(sourcePath, sourceContent);

            final String targetPath = UUID.randomUUID().toString();
            final byte[] targetContent = generateBytes();

            assertNotEquals(sourceContent, targetContent);

            blockingOperator.write(targetPath, targetContent);

            blockingOperator.rename(sourcePath, targetPath);

            assertThatThrownBy(() -> blockingOperator.stat(sourcePath))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));

            assertThat(blockingOperator.read(targetPath)).isEqualTo(sourceContent);

            blockingOperator.delete(sourcePath);
            blockingOperator.delete(targetPath);
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @Nested
    class BlockingListTest {
        @BeforeAll
        public void precondition() {
            final Capability capability = blockingOperator.info.fullCapability;
            assumeTrue(
                    capability.read && capability.write && capability.copy && capability.blocking && capability.list);
        }

        @Test
        public void testBlockingListDir() {
            final String parent = UUID.randomUUID().toString();
            final String path = String.format("%s/%s", parent, UUID.randomUUID().toString());
            final byte[] content = generateBytes();

            blockingOperator.write(path, content);

            final List<Entry> list = blockingOperator.list(parent + "/");
            boolean found = false;
            for (Entry entry : list) {
                if (entry.getPath().equals(path)) {
                    Metadata meta = blockingOperator.stat(path);
                    assertTrue(meta.isFile());
                    assertThat(meta.getContentLength()).isEqualTo(content.length);

                    found = true;
                }
            }
            assertTrue(found);

            blockingOperator.delete(path);
        }

        @Test
        public void testBlockingListDirWithMetakey() {
            final String parent = UUID.randomUUID().toString();
            final String path = String.format("%s/%s", parent, UUID.randomUUID().toString());
            final byte[] content = generateBytes();

            blockingOperator.write(path, content);

            final List<Entry> list = blockingOperator
                    .listWith(parent + "/")
                    .metakeys(
                            Metakey.Mode,
                            Metakey.CacheControl,
                            Metakey.ContentDisposition,
                            Metakey.ContentLength,
                            Metakey.ContentMd5,
                            Metakey.ContentType,
                            Metakey.Etag,
                            Metakey.Version,
                            Metakey.LastModified)
                    .build()
                    .call();
            boolean found = false;
            for (Entry entry : list) {
                if (entry.getPath().equals(path)) {
                    Metadata metadata = entry.getMetadata();
                    assertThat(metadata.getContentLength()).isEqualTo(content.length);
                    assertTrue(metadata.isFile());

                    // We don't care about the value, we just to check there is no panic.
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
            blockingOperator.delete(path);
        }

        /**
         * List dir with metakey complete
         */
        public void testBlockingListDirWithMetakeyComplete() {
            final String parent = UUID.randomUUID().toString();
            final String path = String.format("%s/%s", parent, UUID.randomUUID().toString());
            final byte[] content = generateBytes();

            blockingOperator.write(path, content);

            final List<Entry> list = blockingOperator
                    .listWith(parent + "/")
                    .metakeys(Metakey.Complete)
                    .build()
                    .call();
            boolean found = false;
            for (Entry entry : list) {
                if (entry.getPath().equals(path)) {
                    Metadata metadata = entry.getMetadata();
                    assertThat(metadata.getContentLength()).isEqualTo(content.length);
                    assertTrue(metadata.isFile());

                    // We don't care about the value, we just to check there is no panic.
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
            blockingOperator.delete(path);
        }

        @Test
        public void testBlockingListNonExistDir() {
            final String dir = String.format("%s/", UUID.randomUUID().toString());

            final List<Entry> list = blockingOperator.list(dir);
            assertTrue(list.isEmpty());
        }

        @Test
        public void testBlockingScan() {
            final String parent = UUID.randomUUID().toString();

            final String[] expected = new String[] {
                "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
            };

            for (String path : expected) {
                final byte[] content = generateBytes();
                if (path.endsWith("/")) {
                    blockingOperator.createDir(String.format("%s/%s", parent, path));
                } else {
                    blockingOperator.write(String.format("%s/%s", parent, path), content);
                }
            }

            final List<Entry> list = blockingOperator
                    .listWith(String.format("%s/x/", parent))
                    .delimiter("")
                    .build()
                    .call();
            final Set<String> paths = list.stream().map(Entry::getPath).collect(Collectors.toSet());

            assertTrue(paths.contains(parent + "/x/y"));
            assertTrue(paths.contains(parent + "/x/x/y"));
            assertTrue(paths.contains(parent + "/x/x/x/y"));

            blockingOperator.removeAll(parent + "/");
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
                    blockingOperator.createDir(String.format("%s/%s", parent, path));
                } else {
                    blockingOperator.write(String.format("%s/%s", parent, path), "test_scan");
                }
            }

            blockingOperator.removeAll(parent + "/x/");

            for (String path : expected) {
                if (path.endsWith("/")) {
                    continue;
                }
                assertThatThrownBy(() -> blockingOperator.stat(String.format("%s/%s", parent, path)))
                        .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
            }

            blockingOperator.removeAll(parent + "/");
        }
    }

    /**
     * Generates a byte array of random content.
     */
    public static byte[] generateBytes() {
        final Random random = new Random();
        final int size = random.nextInt(4 * 1024 * 1024) + 1;
        final byte[] content = new byte[size];
        random.nextBytes(content);
        return content;
    }

    protected static boolean isSchemeEnabled(Map<String, String> config) {
        final String turnOn = config.getOrDefault("test", "").toLowerCase();
        return turnOn.equals("on") || turnOn.equals("true");
    }

    protected static Map<String, String> createSchemeConfig(String scheme) {
        final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        final Map<String, String> config = new HashMap<>();
        final String prefix = "opendal_" + scheme.toLowerCase() + "_";
        for (DotenvEntry entry : dotenv.entries()) {
            final String key = entry.getKey().toLowerCase();
            if (key.startsWith(prefix)) {
                config.put(key.substring(prefix.length()), entry.getValue());
            }
        }
        return config;
    }
}
