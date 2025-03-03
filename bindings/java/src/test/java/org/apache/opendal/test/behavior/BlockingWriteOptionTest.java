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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.util.UUID;
import org.apache.opendal.WriteOptions;
import org.junit.jupiter.api.Test;

public class BlockingWriteOptionTest extends BehaviorTestBase {

    @Test
    void testWriteWithCacheControl() {
        assumeTrue(op().info.fullCapability.writeWithCacheControl);

        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        final String cacheControl = "max-age=3600";

        WriteOptions options = WriteOptions.builder().cacheControl(cacheControl).build();
        op().write(path, content, options);

        String actualCacheControl = op().stat(path).getCacheControl();
        assertThat(actualCacheControl).isEqualTo(cacheControl);
    }

    @Test
    void testWriteWithContentType() {
        assumeTrue(op().info.fullCapability.writeWithContentType);

        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        final String contentType = "application/json";

        WriteOptions options = WriteOptions.builder().contentType(contentType).build();
        op().write(path, content, options);

        String actualContentType = op().stat(path).getContentType();
        assertThat(actualContentType).isEqualTo(contentType);
    }

    @Test
    void testWriteWithContentDisposition() {
        assumeTrue(op().info.fullCapability.writeWithContentDisposition);

        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        final String disposition = "attachment; filename=\"test.txt\"";

        WriteOptions options =
                WriteOptions.builder().contentDisposition(disposition).build();
        op().write(path, content, options);

        String actualDisposition = op().stat(path).getContentDisposition();
        assertThat(actualDisposition).isEqualTo(disposition);
    }

    @Test
    void testWriteWithAppend() {
        assumeTrue(op().info.fullCapability.writeCanAppend);

        final String path = UUID.randomUUID().toString();
        final byte[] contentOne = "Test".getBytes();
        final byte[] contentTwo = " Data".getBytes();
        WriteOptions appendOptions = WriteOptions.builder().append(true).build();

        op().write(path, contentOne, appendOptions);
        op().write(path, contentTwo, appendOptions);

        byte[] result = op().read(path);
        assertThat(result.length).isEqualTo(contentOne.length + contentTwo.length);
        assertThat(result).isEqualTo("Test Data".getBytes());
    }
}
