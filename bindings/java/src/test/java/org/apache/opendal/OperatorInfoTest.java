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

package org.apache.opendal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OperatorInfoTest {

    @TempDir
    private static Path tempDir;

    @Test
    public void testBlockingOperatorInfo() {
        Map<String, String> conf = new HashMap<>();
        conf.put("root", tempDir.toString());
        try (BlockingOperator op = new BlockingOperator("fs", conf)) {

            OperatorInfo info = op.info();
            assertNotNull(info);
            assertEquals("fs", info.scheme);

            Capability fullCapability = info.fullCapability;
            assertNotNull(fullCapability);

            assertTrue(fullCapability.read);
            assertTrue(fullCapability.write);
            assertTrue(fullCapability.delete);
            assertTrue(fullCapability.writeCanAppend);

            assertEquals(fullCapability.writeMultiAlignSize, -1);
            assertEquals(fullCapability.writeMultiMaxSize, -1);
            assertEquals(fullCapability.writeMultiMinSize, -1);
            assertEquals(fullCapability.batchMaxOperations, -1);

            Capability nativeCapability = info.nativeCapability;
            assertNotNull(nativeCapability);
        }
    }

    @Test
    public void testOperatorInfo() {
        Map<String, String> conf = new HashMap<>();
        String root = "/opendal/";
        conf.put("root", root);
        try (Operator op = new Operator("memory", conf)) {

            OperatorInfo info = op.info();
            assertNotNull(info);
            assertEquals("memory", info.scheme);
            assertEquals(root, info.root);

            Capability fullCapability = info.fullCapability;
            assertNotNull(fullCapability);

            assertTrue(fullCapability.read);
            assertTrue(fullCapability.write);
            assertTrue(fullCapability.delete);
            assertTrue(!fullCapability.writeCanAppend);

            assertEquals(fullCapability.writeMultiAlignSize, -1);
            assertEquals(fullCapability.writeMultiMaxSize, -1);
            assertEquals(fullCapability.writeMultiMinSize, -1);
            assertEquals(fullCapability.batchMaxOperations, -1);

            Capability nativeCapability = info.nativeCapability;
            assertNotNull(nativeCapability);
        }
    }
}
