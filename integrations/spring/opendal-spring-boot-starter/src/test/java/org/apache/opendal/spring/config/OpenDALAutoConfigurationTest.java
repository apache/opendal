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

import org.apache.opendal.spring.TestApplication;
import org.apache.opendal.spring.core.OpenDALOperations;
import org.apache.opendal.spring.core.OpenDALProperties;
import org.apache.opendal.spring.core.OpenDALSerializerFactory;
import org.apache.opendal.spring.core.OpenDALTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@SpringBootTest(classes = TestApplication.class)
public class OpenDALAutoConfigurationTest {
    @Autowired
    private OpenDALProperties openDALProperties;

    @Autowired
    private OpenDALOperations openDAL;

    @Autowired
    private OpenDALSerializerFactory openDALSerializerFactory;

    @Test
    public void propertiesBeanShouldBeDeclared() {
        Assertions.assertNotNull(openDALProperties);

        Assertions.assertEquals("fs", openDALProperties.getSchema());
        Assertions.assertEquals(1, openDALProperties.getConf().size());
        Assertions.assertEquals("/tmp", openDALProperties.getConf().get("root"));
    }

    @Test
    public void beanShouldBeDeclared() {
        Assertions.assertNotNull(openDAL);
        Assertions.assertInstanceOf(OpenDALTemplate.class, openDAL);
    }

    @Test
    public void serializerFactoryBeanShouldBeDeclared() {
        Assertions.assertNotNull(openDALSerializerFactory);
    }
}
