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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;

public class StepsTest {
    BlockingOperator op;

    @Given("A new OpenDAL Blocking Operator")
    public void a_new_open_dal_blocking_operator() {
        Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        op = new BlockingOperator("Memory", params);
    }

    @When("Blocking write path {string} with content {string}")
    public void blocking_write_path_test_with_content_hello_world(String path, String content) {
        op.write(path, content);
    }

    @Then("The blocking file {string} should exist")
    public void the_blocking_file_test_should_exist(String path) {
        @Cleanup Metadata metadata = op.stat(path);
        assertNotNull(metadata);
    }

    @Then("The blocking file {string} entry mode must be file")
    public void the_blocking_file_test_entry_mode_must_be_file(String path) {
        @Cleanup Metadata metadata = op.stat(path);
        assertTrue(metadata.isFile());
    }

    @Then("The blocking file {string} content length must be {int}")
    public void the_blocking_file_test_content_length_must_be_13(String path, int length) {
        @Cleanup Metadata metadata = op.stat(path);
        assertEquals(metadata.getContentLength(), length);
    }

    @Then("The blocking file {string} must have content {string}")
    public void the_blocking_file_test_must_have_content_hello_world(String path, String content) {
        byte[] readContent = op.read(path);
        assertThat(readContent).isEqualTo(content.getBytes(StandardCharsets.UTF_8));
    }
}
