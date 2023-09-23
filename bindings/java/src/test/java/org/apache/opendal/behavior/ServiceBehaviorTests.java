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

package org.apache.opendal.behavior;

import java.io.File;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.condition.EnabledIf;

@Slf4j
class MemoryTest extends AbstractBehaviorTest {
    public MemoryTest() {
        super("memory", defaultSchemeConfig());
    }

    private static Map<String, String> defaultSchemeConfig() {
        final Map<String, String> config = createSchemeConfig("memory");
        if (!isSchemeEnabled(config)) {
            log.info("Running MemoryTest with default config.");
            config.clear();
            config.put("test", "on");
            config.put("root", "/tmp");
        }
        return config;
    }
}

@Slf4j
class FsTest extends AbstractBehaviorTest {
    public FsTest() {
        super("fs", schemeConfig());
    }

    private static Map<String, String> schemeConfig() {
        final Map<String, String> config = createSchemeConfig("fs");
        if (!isSchemeEnabled(config)) {
            log.info("Running FsTest with default config.");
            config.clear();

            final File tempDir = Files.newTemporaryFolder();
            tempDir.deleteOnExit();
            config.put("test", "on");
            config.put("root", tempDir.getAbsolutePath());
        }
        return config;
    }
}

@EnabledIf("enabled")
class RedisTest extends AbstractBehaviorTest {
    public RedisTest() {
        super("redis");
    }

    private static boolean enabled() {
        return isSchemeEnabled(createSchemeConfig("redis"));
    }
}

@EnabledIf("enabled")
class S3Test extends AbstractBehaviorTest {
    public S3Test() {
        super("s3");
    }

    private static boolean enabled() {
        return isSchemeEnabled(createSchemeConfig("s3"));
    }
}
