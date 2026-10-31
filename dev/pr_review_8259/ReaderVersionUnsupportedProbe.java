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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorReader;
import org.apache.opendal.ReaderOptions;
import org.apache.opendal.ServiceConfig;

public final class ReaderVersionUnsupportedProbe {
    private ReaderVersionUnsupportedProbe() {}

    public static void main(String[] args) throws Exception {
        final Path root = Files.createTempDirectory("opendal-8259-fs-");
        try (Operator op =
                Operator.of(ServiceConfig.Fs.builder().root(root.toString()).build())) {
            final byte[] content = "version-probe".getBytes(StandardCharsets.UTF_8);
            op.write("file", content);
            try (OperatorReader reader = op.createReader("file")) {
                if (!java.util.Arrays.equals(content, reader.read(0, content.length))) {
                    throw new AssertionError("default Fs reader did not return the written content");
                }
            }

            try {
                try (OperatorReader reader = op.createReader(
                        "file", ReaderOptions.builder().version("version-one").build())) {
                    reader.read(0, content.length);
                }
                throw new AssertionError("Fs unexpectedly accepted a versioned read");
            } catch (OpenDALException err) {
                if (err.getCode() != OpenDALException.Code.Unsupported) {
                    throw new AssertionError("expected Unsupported for Fs version option, got " + err.getCode(), err);
                }
            }
        } finally {
            Files.deleteIfExists(root.resolve("file"));
            Files.deleteIfExists(root);
        }

        System.out.println("PASS: Fs default read succeeds and versioned reader is Unsupported");
    }
}
