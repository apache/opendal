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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.opendal.Capability;
import org.apache.opendal.Entry;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.args.OpList.Metakey;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BlockingListTest extends BehaviorTestBase {
    @BeforeAll
    public void precondition() {
        final Capability capability = blockingOp().info.fullCapability;
        assumeTrue(capability.read && capability.write && capability.copy && capability.blocking && capability.list);
    }

    @Test
    public void testBlockingListDir() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        blockingOp().write(path, content);

        final List<Entry> list = blockingOp().list(parent + "/");
        boolean found = false;
        for (Entry entry : list) {
            if (entry.getPath().equals(path)) {
                Metadata meta = blockingOp().stat(path);
                assertTrue(meta.isFile());
                assertThat(meta.getContentLength()).isEqualTo(content.length);

                found = true;
            }
        }
        assertTrue(found);

        blockingOp().delete(path);
    }

    @Test
    public void testBlockingListDirWithMetakey() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        blockingOp().write(path, content);

        final List<Entry> list = blockingOp()
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
        blockingOp().delete(path);
    }

    /**
     * List dir with metakey complete
     */
    public void testBlockingListDirWithMetakeyComplete() {
        final String parent = UUID.randomUUID().toString();
        final String path = String.format("%s/%s", parent, UUID.randomUUID());
        final byte[] content = generateBytes();

        blockingOp().write(path, content);

        final List<Entry> list = blockingOp()
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
        blockingOp().delete(path);
    }

    @Test
    public void testBlockingListNonExistDir() {
        final String dir = String.format("%s/", UUID.randomUUID());

        final List<Entry> list = blockingOp().list(dir);
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
                blockingOp().createDir(String.format("%s/%s", parent, path));
            } else {
                blockingOp().write(String.format("%s/%s", parent, path), content);
            }
        }

        final List<Entry> list = blockingOp()
                .listWith(String.format("%s/x/", parent))
                .delimiter("")
                .build()
                .call();
        final Set<String> paths = list.stream().map(Entry::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains(parent + "/x/y"));
        assertTrue(paths.contains(parent + "/x/x/y"));
        assertTrue(paths.contains(parent + "/x/x/x/y"));

        blockingOp().removeAll(parent + "/");
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
                blockingOp().createDir(String.format("%s/%s", parent, path));
            } else {
                blockingOp().write(String.format("%s/%s", parent, path), "test_scan");
            }
        }

        blockingOp().removeAll(parent + "/x/");

        for (String path : expected) {
            if (path.endsWith("/")) {
                continue;
            }
            assertThatThrownBy(() -> blockingOp().stat(String.format("%s/%s", parent, path)))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
        }

        blockingOp().removeAll(parent + "/");
    }
}
