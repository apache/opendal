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

package org.apache.opendal.test;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.Cleanup;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Layer;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.layer.ConcurrentLimitLayer;
import org.apache.opendal.layer.HotpathLayer;
import org.apache.opendal.layer.LoggingLayer;
import org.apache.opendal.layer.MimeGuessLayer;
import org.apache.opendal.layer.RetryLayer;
import org.apache.opendal.layer.ThrottleLayer;
import org.apache.opendal.layer.TimeoutLayer;
import org.junit.jupiter.api.Test;

public class LayerTest {
    @Test
    void testOperatorWithRetryLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer retryLayer = RetryLayer.builder().build();
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(retryLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testOperatorWithConcurrentLimitLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer concurrentLimitLayer = new ConcurrentLimitLayer(1024);
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(concurrentLimitLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testOperatorWithMimeGuessLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer mimeGuessLayer = new MimeGuessLayer();
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(mimeGuessLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testOperatorWithTimeoutLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer timeoutLayer = new TimeoutLayer();
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(timeoutLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testOperatorWithLoggingLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer loggingLayer = new LoggingLayer();
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(loggingLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testOperatorWithThrottleLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer throttleLayer = new ThrottleLayer(1024, 1024);
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(throttleLayer);
        assertThat(layeredOp.info).isNotNull();
    }

    @Test
    void testOperatorWithHotpathLayer() {
        final ServiceConfig.Memory memory =
                ServiceConfig.Memory.builder().root("/opendal/").build();
        final Layer hotpathLayer = new HotpathLayer();
        @Cleanup final AsyncOperator op = AsyncOperator.of(memory);
        @Cleanup final AsyncOperator layeredOp = op.layer(hotpathLayer);
        assertThat(layeredOp.info).isNotNull();
    }
}
