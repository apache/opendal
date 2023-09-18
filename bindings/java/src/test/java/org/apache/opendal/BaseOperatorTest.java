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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.opendal.condition.OpenDALExceptionCondition;
import org.apache.opendal.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseOperatorTest {

    protected Operator op;

    protected abstract String schema();

    @BeforeAll
    public void init() {
        String schema = this.schema();
        Optional<Operator> optional = Utils.init(schema);
        assertTrue(optional.isPresent());
        op = optional.get();
    }

    @AfterAll
    public void destroy() {
        if (op != null) {
            op.close();
        }
    }

    @Test
    public final void testWrite() throws Exception {
        String path = UUID.randomUUID().toString();
        byte[] content = Utils.generateBytes();
        op.write(path, content).join();

        Metadata metadata = op.stat(path).get();

        assertEquals(content.length, metadata.getContentLength());

        op.delete(path).join();
        assertThatThrownBy(() -> op.stat(path).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @Test
    public final void testRead() throws Exception {
        Metadata metadata = op.stat("").get();
        assertTrue(!metadata.isFile());

        String path = UUID.randomUUID().toString();
        assertThatThrownBy(() -> op.stat(path).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));

        String content = Utils.generateString();
        op.write(path, content).join();

        assertThat(op.read(path).join()).isEqualTo(content);

        op.delete(path).join();
        assertThatThrownBy(() -> op.stat(path).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @Test
    public void testAppend() {
        String path = UUID.randomUUID().toString();
        String[] trunks = new String[] {Utils.generateString(), Utils.generateString(), Utils.generateString()};

        for (int i = 0; i < trunks.length; i++) {
            op.append(path, trunks[i]).join();
            String expected = Arrays.stream(trunks).limit(i + 1).collect(Collectors.joining());
            assertThat(op.read(path).join()).isEqualTo(expected);
        }

        // write overwrite existing content
        String newAttempt = Utils.generateString();
        op.write(path, newAttempt).join();
        assertThat(op.read(path).join()).isEqualTo(newAttempt);

        for (int i = 0; i < trunks.length; i++) {
            op.append(path, trunks[i]).join();
            String expected = Arrays.stream(trunks).limit(i + 1).collect(Collectors.joining());
            assertThat(op.read(path).join()).isEqualTo(newAttempt + expected);
        }
    }
}
