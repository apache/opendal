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
import org.apache.opendal.spring.core.DefaultOpenDALSerializerFactory;
import org.apache.opendal.spring.core.OpenDALProperties;
import org.apache.opendal.spring.core.OpenDALSerializerFactory;
import org.apache.opendal.spring.core.OpenDALTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
@SpringBootTest(classes = TestApplication.class)
public class OpenDALAutoConfigurationTest {
    @Autowired
    private OpenDALProperties openDALProperties;

    @Autowired
    private OpenDALTemplate openDALTemplate;

    @Autowired
    private AsyncOperator asyncOperator;

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
        Assertions.assertNotNull(openDALTemplate);
        Assertions.assertInstanceOf(OpenDALTemplate.class, openDALTemplate);
    }

    @Test
    public void asyncOperatorShouldBeDeclared() {
        Assertions.assertNotNull(asyncOperator);
    }

    @Test
    public void serializerFactoryBeanShouldBeDeclared() {
        Assertions.assertNotNull(openDALSerializerFactory);
    }

    private ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OpenDALAutoConfiguration.class))
            .withPropertyValues("spring.opendal.schema=memory", "spring.opendal.conf.root=/");
    }

    @Test
    public void customAsyncOperatorShouldOverrideAutoConfiguration() {
        contextRunner()
            .withUserConfiguration(CustomAsyncOperatorConfiguration.class)
            .run(context -> {
                assertThat(context).hasSingleBean(AsyncOperator.class);
                assertThat(context).hasSingleBean(OpenDALTemplate.class);
                AsyncOperator operator = context.getBean(AsyncOperator.class);
                assertThat(operator).isSameAs(context.getBean("customAsyncOperator"));

                OpenDALTemplate template = context.getBean(OpenDALTemplate.class);
                template.ops(Person.class).write("custom-op", new Person("Bob", 2));
                assertThat(operator.read("custom-op").join()).isNotEmpty();
            });
    }

    @Test
    public void customOpenDALTemplateShouldOverrideAutoConfiguration() {
        contextRunner()
            .withUserConfiguration(CustomOpenDALTemplateConfiguration.class)
            .run(context -> {
                assertThat(context).hasSingleBean(OpenDALTemplate.class);
                assertThat(context.getBean(OpenDALTemplate.class))
                    .isSameAs(context.getBean("customOpenDALTemplate"));
            });
    }

    @Test
    public void customSerializerFactoryShouldOverrideAutoConfiguration() {
        contextRunner()
            .withUserConfiguration(CustomSerializerFactoryConfiguration.class)
            .run(context -> {
                assertThat(context).hasSingleBean(OpenDALSerializerFactory.class);
                assertThat(context.getBean(OpenDALSerializerFactory.class))
                    .isSameAs(context.getBean("customSerializerFactory"));
            });
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomAsyncOperatorConfiguration {
        @Bean
        AsyncOperator customAsyncOperator() {
            return AsyncOperator.of("memory", Map.of());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomOpenDALTemplateConfiguration {
        @Bean
        OpenDALTemplate customOpenDALTemplate(
            AsyncOperator asyncOperator, OpenDALSerializerFactory serializerFactory) {
            return new OpenDALTemplate(asyncOperator, serializerFactory);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomSerializerFactoryConfiguration {
        @Bean
        OpenDALSerializerFactory customSerializerFactory() {
            return new DefaultOpenDALSerializerFactory();
        }
    }
}
