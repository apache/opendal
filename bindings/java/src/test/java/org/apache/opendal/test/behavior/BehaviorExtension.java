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

package org.apache.opendal.test.behavior;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.opendal.BlockingOperator;
import org.apache.opendal.Operator;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

@Slf4j
public class BehaviorExtension implements BeforeAllCallback, AfterAllCallback {
    public Operator operator;
    public BlockingOperator blockingOperator;

    @Override
    public void beforeAll(ExtensionContext context) {
        final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        final String scheme = dotenv.get("OPENDAL_TEST");
        if (scheme != null) {
            final Map<String, String> config = new HashMap<>();
            final String prefix = "opendal_" + scheme.toLowerCase() + "_";
            for (DotenvEntry entry : dotenv.entries()) {
                final String key = entry.getKey().toLowerCase();
                if (key.startsWith(prefix)) {
                    config.put(key.substring(prefix.length()), entry.getValue());
                }
            }
            this.operator = Operator.of(scheme, config);
            this.blockingOperator = BlockingOperator.of(scheme, config);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (operator != null) {
            operator.close();
            operator = null;
        }

        if (blockingOperator != null) {
            blockingOperator.close();
            blockingOperator = null;
        }
    }
}
