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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.opendal.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OperatorTest {
    @TempDir
    private static Path tempDir;

    @Test
    public void testCreateAndDelete() {
        Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        @Cleanup Operator op = new Operator("Memory", params);

        op.write("testCreateAndDelete", "Odin").join();
        assertThat(op.read("testCreateAndDelete").join()).isEqualTo("Odin");
        op.delete("testCreateAndDelete").join();
        assertThatThrownBy(() -> op.stat("testCreateAndDelete").join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.NotFound));
    }

    @Test
    public void testAppendManyTimes() {
        Map<String, String> params = new HashMap<>();
        params.put("root", tempDir.toString());
        @Cleanup Operator op = new Operator("fs", params);

        String[] trunks = new String[] {"first trunk", "second trunk", "third trunk"};

        for (int i = 0; i < trunks.length; i++) {
            op.append("testAppendManyTimes", trunks[i]).join();
            String expected = Arrays.stream(trunks).limit(i + 1).collect(Collectors.joining());
            assertThat(op.read("testAppendManyTimes").join()).isEqualTo(expected);
        }

        // write overwrite existing content
        op.write("testAppendManyTimes", "new attempt").join();
        assertThat(op.read("testAppendManyTimes").join()).isEqualTo("new attempt");

        for (int i = 0; i < trunks.length; i++) {
            op.append("testAppendManyTimes", trunks[i]).join();
            String expected = Arrays.stream(trunks).limit(i + 1).collect(Collectors.joining());
            assertThat(op.read("testAppendManyTimes").join()).isEqualTo("new attempt" + expected);
        }
    }
}
