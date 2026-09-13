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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ExecutorShutdownTest {
    @TempDir
    Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"blocking", "async"})
    void testJvmExitsAfterClosingOperator(String mode) throws Exception {
        final String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        final Path java = Paths.get(System.getProperty("java.home"), "bin", executable);
        final Path log = tempDir.resolve(mode + ".log");
        final Process process = new ProcessBuilder(
                        java.toString(),
                        "-Xcheck:jni",
                        "-Djava.library.path=" + System.getProperty("java.library.path"),
                        "-cp",
                        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                        Application.class.getName(),
                        mode)
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
        assertThat(output).contains("OPERATORS_CLOSED");
        assertThat(exited).as("JVM must exit after main returns: %s", output).isTrue();
        assertThat(process.exitValue()).as(output).isZero();
    }

    public static class Application {
        public static void main(String[] args) {
            final byte[] content = "Hello, OpenDAL!".getBytes(StandardCharsets.UTF_8);
            if (args[0].equals("blocking")) {
                try (final Operator op = Operator.of("memory", Collections.emptyMap())) {
                    op.write("example.txt", content);
                    assertThat(op.read("example.txt")).isEqualTo(content);
                }
            } else {
                try (final AsyncOperator op = AsyncOperator.of("memory", Collections.emptyMap())) {
                    op.write("example.txt", content).join();
                    assertThat(op.read("example.txt").join()).isEqualTo(content);
                    assertThatThrownBy(() -> op.read("missing.txt").join()).hasCauseInstanceOf(OpenDALException.class);
                    assertThat(op.read("example.txt").join()).isEqualTo(content);
                }
            }
            System.out.println("OPERATORS_CLOSED");
        }
    }
}
