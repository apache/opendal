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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import lombok.Cleanup;
import org.junit.jupiter.api.Test;

public class LayerSpecTest {
    @Test
    public void testOperatorWithLayers() {
        final Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        final List<LayerSpec> layerSpecs = new ArrayList<>();
        layerSpecs.add(new BlockingLayerSpec());
        layerSpecs.add(RetryLayerSpec.builder().build());
        @Cleanup Operator op = new Operator("Memory", layerSpecs, params);

        op.write("testCreateAndDelete", "Odin").join();
        assertThat(op.read("testCreateAndDelete").join()).isEqualTo("Odin");
        op.delete("testCreateAndDelete").join();
        op.stat("testCreateAndDelete")
                .handle((r, e) -> {
                    assertThat(r).isNull();
                    assertThat(e).isInstanceOf(CompletionException.class).hasCauseInstanceOf(OpenDALException.class);
                    OpenDALException.Code code = ((OpenDALException) e.getCause()).getCode();
                    assertThat(code).isEqualTo(OpenDALException.Code.NotFound);
                    return null;
                })
                .join();
    }

    @Test
    public void testBlockingOperatorWithLayers() {
        final Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        final List<LayerSpec> layerSpecs = new ArrayList<>();
        layerSpecs.add(new BlockingLayerSpec());
        layerSpecs.add(RetryLayerSpec.builder().build());
        @Cleanup BlockingOperator op = new BlockingOperator("Memory", layerSpecs, params);

        op.write("testCreateAndDelete", "Odin");
        assertThat(op.read("testCreateAndDelete")).isEqualTo("Odin");
        op.delete("testCreateAndDelete");
        assertThatExceptionOfType(OpenDALException.class)
                .isThrownBy(() -> op.stat("testCreateAndDelete"))
                .extracting(OpenDALException::getCode)
                .isEqualTo(OpenDALException.Code.NotFound);
    }
}
