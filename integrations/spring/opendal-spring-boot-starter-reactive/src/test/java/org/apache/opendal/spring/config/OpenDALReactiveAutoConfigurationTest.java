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

import org.apache.opendal.spring.TestReactiveApplication;
import org.apache.opendal.spring.core.OpenDALProperties;
import org.apache.opendal.spring.core.ReactiveOpenDALOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@SpringBootTest(classes = TestReactiveApplication.class)
public class OpenDALReactiveAutoConfigurationTest {

    @Autowired
    private OpenDALProperties openDALProperties;

    @Autowired
    private ReactiveOpenDALOperations openDALReactive;

    @Test
    public void propertiesBeanShouldBeDeclared() {
        Assertions.assertNotNull(openDALProperties);

        Assertions.assertEquals("fs", openDALProperties.getSchema());
        Assertions.assertEquals(1, openDALProperties.getConf().size());
        Assertions.assertEquals("/tmp", openDALProperties.getConf().get("root"));
    }

    @Test
    public void reactiveBeanShouldBeDeclared() {
        Assertions.assertNotNull(openDALReactive);
        Assertions.assertInstanceOf(ReactiveOpenDALOperations.class, openDALReactive);
    }

}
