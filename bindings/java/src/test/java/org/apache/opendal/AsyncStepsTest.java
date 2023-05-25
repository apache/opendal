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

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AsyncStepsTest {
    Operator op;

    @Given("A new OpenDAL Async Operator")
    public void a_new_open_dal_async_operator() {
        Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        op = new Operator("Memory", params);
    }

    @When("Async write path {string} with content {string}")
    public void async_write_path_test_with_content_hello_world(String fileName, String content) {
        op.write(fileName, content).join();
    }

    @Then("The async file {string} should exist")
    public void the_async_file_test_should_exist(String fileName) {
        Metadata metadata = op.stat(fileName).join();
        assertNotNull(metadata);
    }

    @Then("The async file {string} entry mode must be file")
    public void the_async_file_test_entry_mode_must_be_file(String fileName) {
        Metadata metadata = op.stat(fileName).join();
        assertTrue(metadata.isFile());
    }

    @Then("The async file {string} content length must be {int}")
    public void the_async_file_test_content_length_must_be_13(String fileName, int length) {
        Metadata metadata = op.stat(fileName).join();
        assertEquals(metadata.getContentLength(), length);
    }

    @Then("The async file {string} must have content {string}")
    public void the_async_file_test_must_have_content_hello_world(String fileName, String content) {
        String readContent = op.read(fileName).join();
        assertEquals(content, readContent);
    }
}
