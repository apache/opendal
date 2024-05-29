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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import org.apache.opendal.Capability;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BlockingReadOnlyTest extends BehaviorTestBase {

    private static final String NORMAL_FILE_NAME = "normal_file.txt";
    private static final String NORMAL_DIR_NAME = "normal_dir/";
    private static final String SPECIAL_FILE_NAME = "special_file  !@#$%^&()_+-=;',.txt";
    private static final String SPECIAL_DIR_NAME = "special_dir  !@#$%^&()_+-=;',/";
    private static final String FILE_SHA256_DIGEST = "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5";
    private static final long FILE_LENGTH = 30482L;

    @BeforeAll
    public void precondition() {
        final Capability capability = op().info.fullCapability;
        assumeTrue(capability.read && !capability.write && capability.blocking);
    }

    /**
     * Stat normal file and dir should return metadata
     */
    @Test
    public void testBlockingReadOnlyStatFileAndDir() {
        final Metadata fileMeta = op().stat(NORMAL_FILE_NAME);
        assertTrue(fileMeta.isFile());
        assertEquals(FILE_LENGTH, fileMeta.getContentLength());

        final Metadata dirMeta = op().stat(NORMAL_DIR_NAME);
        assertTrue(dirMeta.isDir());
    }

    /**
     * Stat special file and dir should return metadata
     */
    @Test
    public void testBlockingReadOnlyStatSpecialChars() {
        final Metadata fileMeta = op().stat(SPECIAL_FILE_NAME);
        assertTrue(fileMeta.isFile());
        assertEquals(FILE_LENGTH, fileMeta.getContentLength());

        final Metadata dirMeta = op().stat(SPECIAL_DIR_NAME);
        assertTrue(dirMeta.isDir());
    }

    /**
     * Stat not exist file should return NotFound
     */
    @Test
    public void testBlockingReadOnlyStatNotExist() {
        final String path = UUID.randomUUID().toString();
        assertThatThrownBy(() -> op().stat(path)).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
    }

    /**
     * Read full content should match.
     */
    @Test
    public void testBlockingReadonlyReadFull() throws NoSuchAlgorithmException {
        final byte[] content = op().read(NORMAL_FILE_NAME);
        assertEquals(FILE_LENGTH, content.length);
        assertEquals(FILE_SHA256_DIGEST, sha256Digest(content));
    }

    /**
     * Read not exist file should return NotFound
     */
    @Test
    public void testBlockingReadOnlyReadNotExist() {
        final String path = UUID.randomUUID().toString();
        assertThatThrownBy(() -> op().read(path)).is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.NotFound));
    }
}
