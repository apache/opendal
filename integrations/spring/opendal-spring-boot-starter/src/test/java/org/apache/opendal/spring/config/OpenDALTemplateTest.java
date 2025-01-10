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

package org.apache.opendal.spring.config;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.spring.TestApplication;
import org.apache.opendal.spring.core.OpenDALOperations;
import org.apache.opendal.spring.core.OpenDALTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

@SpringJUnitConfig
@SpringBootTest(classes = TestApplication.class)
public class OpenDALTemplateTest {
    @Autowired
    private OpenDALTemplate openDALTemplate;

    @Autowired
    private AsyncOperator asyncOperator;

    @Test
    public void simpleTest() {
        String path = "my";
        OpenDALOperations<Person> ops = openDALTemplate.ops(Person.class);
        ops.write(path, new Person("Alice", 1));
        Person person = ops.read(path);
        Assertions.assertEquals("Alice", person.name());
        Assertions.assertEquals(1, person.age());
        String content = new String(asyncOperator.blocking().read(path), StandardCharsets.UTF_8);
        Assertions.assertEquals("""
            {"name":"Alice","age":1}""", content);
        ops.delete(path);
        Assertions.assertThrows(Exception.class, () -> ops.read(path));
    }

    @Test
    public void simpleAsyncTest() throws ExecutionException, InterruptedException {
        String path = "my";
        OpenDALOperations<Person> ops = openDALTemplate.ops(Person.class);
        ops.writeAsync(path, new Person("Alice", 1)).get();
        Person person = ops.readAsync(path).get();
        Assertions.assertEquals("Alice", person.name());
        Assertions.assertEquals(1, person.age());
        String content = new String(asyncOperator.read(path).get(), StandardCharsets.UTF_8);
        Assertions.assertEquals("""
            {"name":"Alice","age":1}""", content);
        ops.deleteAsync(path).get();
        Assertions.assertThrows(Exception.class, () -> ops.readAsync(path).get());
    }
}
