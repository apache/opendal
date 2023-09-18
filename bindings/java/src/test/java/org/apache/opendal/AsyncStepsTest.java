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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import org.apache.opendal.condition.OpenDALExceptionCondition;

public class AsyncStepsTest {
    Operator op;

    @Given("A new OpenDAL Async Operator")
    public void a_new_open_dal_async_operator() {
        Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        op = new Operator("Memory", params);
    }

    @When("Async write path {string} with content {string}")
    public void async_write_path_test_with_content_hello_world(String path, String content) {
        op.write(path, content).join();
    }

    @Then("The async file {string} should exist")
    public void the_async_file_test_should_exist(String path) {
        @Cleanup Metadata metadata = op.stat(path).join();
        assertNotNull(metadata);
    }

    @Then("The async file {string} entry mode must be file")
    public void the_async_file_test_entry_mode_must_be_file(String path) {
        @Cleanup Metadata metadata = op.stat(path).join();
        assertTrue(metadata.isFile());
    }

    @Then("The async file {string} content length must be {int}")
    public void the_async_file_test_content_length_must_be_13(String path, int length) {
        @Cleanup Metadata metadata = op.stat(path).join();
        assertEquals(metadata.getContentLength(), length);
    }

    @Then("The async file {string} must have content {string}")
    public void the_async_file_test_must_have_content_hello_world(String path, String content) {
        String readContent = op.read(path).join();
        assertEquals(content, readContent);
    }

    @Then("The presign operation should success or raise exception Unsupported")
    public void the_presign_operation_should_success_or_raise_exception_unsupported() {
        assertThatThrownBy(
                        () -> op.presignStat("test.txt", Duration.ofSeconds(10)).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unsupported));
        assertThatThrownBy(
                        () -> op.presignRead("test.txt", Duration.ofSeconds(10)).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unsupported));
        assertThatThrownBy(() ->
                        op.presignWrite("test.txt", Duration.ofSeconds(10)).join())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unsupported));
    }
}
