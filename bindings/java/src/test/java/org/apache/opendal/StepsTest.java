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

public class StepsTest {

    Operator operator;

    @Given("A new OpenDAL Blocking Operator")
    public void a_new_open_dal_blocking_operator() {
        Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        this.operator = new Operator("Memory", params);
    }

    @When("Blocking write path {string} with content {string}")
    public void blocking_write_path_test_with_content_hello_world(String fileName, String content) {
        this.operator.write(fileName, content);
    }


    @Then("The blocking file {string} should exist")
    public void the_blocking_file_test_should_exist(String content) {

    }


    @Then("The blocking file {string} entry mode must be file")
    public void the_blocking_file_test_entry_mode_must_be_file(String fileName) {

    }

    @Then("The blocking file {string} content length must be {int}")
    public void the_blocking_file_test_content_length_must_be_13(String fileName, int length) {
        String content = this.operator.read(fileName);

        assertEquals(content.length(), length);
    }

    @Then("The blocking file {string} must have content {string}")
    public void the_blocking_file_test_must_have_content_hello_world(String fileName, String content) {
        String readContent = this.operator.read(fileName);

        assertEquals(content, readContent);
    }


}
