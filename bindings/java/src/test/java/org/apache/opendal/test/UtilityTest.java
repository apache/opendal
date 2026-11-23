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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.opendal.OpenDAL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class UtilityTest {
    @TempDir
    Path tempDir;

    @Test
    public void testLoadEnabledServices() {
        final Collection<String> enabledServices = OpenDAL.enabledServices();
        assertThat(enabledServices).contains("memory", "fs", "gcs-grpc", "goosefs", "minio", "r2");
        if (!System.getProperty("os.name").startsWith("Windows")) {
            assertThat(enabledServices).contains("sftp");
        }
    }

    @Test
    public void testLoadEnabledServicesHasNoJniLocalRefWarning() throws Exception {
        final String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        final Path java = Paths.get(System.getProperty("java.home"), "bin", executable);
        final Path log = tempDir.resolve("enabled-services-check-jni.log");
        final Process process = new ProcessBuilder(
                        java.toString(),
                        "-Xcheck:jni",
                        "-Djava.library.path=" + System.getProperty("java.library.path"),
                        "-cp",
                        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                        EnabledServicesCheck.class.getName())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        final boolean exited;
        try {
            exited = process.waitFor(15, TimeUnit.SECONDS);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
        final String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
        assertThat(exited).as("JVM must exit cleanly: %s", output).isTrue();
        assertThat(process.exitValue()).as(output).isZero();
        assertThat(output).contains("ENABLED_SERVICES_LOADED");
        assertThat(output).doesNotContain("WARNING: JNI local refs");
    }

    public static class EnabledServicesCheck {
        public static void main(String[] args) {
            final Collection<String> enabledServices = OpenDAL.enabledServices();
            assertThat(enabledServices).contains("memory", "gcs-grpc", "goosefs", "minio", "r2");
            System.out.println("ENABLED_SERVICES_LOADED");
        }
    }
}
