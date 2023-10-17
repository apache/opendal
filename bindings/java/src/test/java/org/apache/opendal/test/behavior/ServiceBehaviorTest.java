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
import org.junit.jupiter.api.condition.EnabledIf;

@EnabledIf("enabled")
class ServiceBehaviorTest extends AbstractBehaviorTest {
    protected ServiceBehaviorTest() {
        super(lookupScheme(), createSchemeConfig());
    }

    private static boolean enabled() {
        return lookupScheme() != null;
    }

    private static String lookupScheme() {
        final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        return dotenv.get("OPENDAL_TEST");
    }

    private static Map<String, String> createSchemeConfig() {
        final String scheme = lookupScheme();
        final Map<String, String> config = new HashMap<>();
        if (scheme != null) {
            final String prefix = "opendal_" + scheme.toLowerCase() + "_";
            final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
            for (DotenvEntry entry : dotenv.entries()) {
                final String key = entry.getKey().toLowerCase();
                if (key.startsWith(prefix)) {
                    config.put(key.substring(prefix.length()), entry.getValue());
                }
            }
        }
        return config;
    }
}
