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
import org.apache.opendal.spring.core.DefaultOpenDALSerializerFactory;
import org.apache.opendal.spring.core.OpenDALOperations;
import org.apache.opendal.spring.core.OpenDALProperties;
import org.apache.opendal.spring.core.OpenDALSerializerFactory;
import org.apache.opendal.spring.core.ReactiveOpenDALOperations;
import org.apache.opendal.spring.core.ReactiveOpenDALTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(OpenDALProperties.class)
public class OpenDALReactiveAutoConfiguration {
    private final OpenDALProperties openDALProperties;

    public OpenDALReactiveAutoConfiguration(OpenDALProperties openDALProperties) {
        this.openDALProperties = openDALProperties;
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveOpenDALOperations.class)
    public ReactiveOpenDALTemplate reactiveOpendalTemplate(AsyncOperator asyncOperator, OpenDALSerializerFactory openDALSerializerFactory) {
        return new ReactiveOpenDALTemplate(asyncOperator, openDALSerializerFactory);
    }

    @Bean
    @ConditionalOnMissingBean(OpenDALOperations.class)
    public AsyncOperator asyncOperator(OpenDALProperties openDALProperties) {
        return AsyncOperator.of(openDALProperties.getSchema(), openDALProperties.getConf());
    }

    @Bean
    @ConditionalOnMissingBean(name = "openDALSerializerFactory")
    public OpenDALSerializerFactory openDALSerializerFactory() {
        return new DefaultOpenDALSerializerFactory();
    }
}
