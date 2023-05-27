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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BlockingOperatorTest {
    private BlockingOperator op;

    @BeforeEach
    public void init() {
        Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        this.op = new BlockingOperator("Memory", params);
    }

    @AfterEach
    public void clean() {
        this.op.close();
    }

    @Test
    public void testStatNotExistFile() {
        assertThatExceptionOfType(OpenDALException.class)
                .isThrownBy(() -> op.stat("nonexistence"))
                .extracting(OpenDALException::getCode)
                .isEqualTo(OpenDALException.Code.NotFound);
    }

    @Test
    public void testCreateAndDelete() {
        op.write("testCreateAndDelete", "Odin");
        assertThat(op.read("testCreateAndDelete")).isEqualTo("Odin");
        op.delete("testCreateAndDelete");
        assertThatExceptionOfType(OpenDALException.class)
                .isThrownBy(() -> op.stat("testCreateAndDelete"))
                .extracting(OpenDALException::getCode)
                .isEqualTo(OpenDALException.Code.NotFound);
    }
}
