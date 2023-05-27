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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

public enum Environment {
    INSTANCE;

    public static final String UNKNOWN = "<unknown>";
    private String classifier = UNKNOWN;
    private String projectVersion = UNKNOWN;

    static {
        ClassLoader classLoader = Environment.class.getClassLoader();
        try (InputStream is = classLoader.getResourceAsStream("bindings.properties")) {
            final Properties properties = new Properties();
            properties.load(is);
            INSTANCE.classifier = properties.getProperty("jniClassifier", UNKNOWN);
            INSTANCE.projectVersion = properties.getProperty("project.version", UNKNOWN);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot load environment properties file", e);
        }
    }

    /**
     * Returns the classifier of the compiled environment.
     *
     * @return The classifier of the compiled environment.
     */
    public static String getClassifier() {
        return INSTANCE.classifier;
    }

    /**
     * Returns the version of the code as String.
     *
     * @return The project version string.
     */
    public static String getVersion() {
        return INSTANCE.projectVersion;
    }

}
