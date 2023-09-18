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

package org.apache.opendal.utils;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.opendal.Operator;

public class Utils {

    public static final String ENV_NAME = ".env";
    public static final String CONF_PREFIX = "opendal_";
    public static final String CONF_TURN_ON_TEST = "test";
    public static final String CONF_ROOT = "root";
    public static final String CONF_RANDOM_ROOT_FLAG = "OPENDAL_DISABLE_RANDOM_ROOT";

    /**
     * Initializes the Service with the given schema.
     *
     * @param schema the schema to initialize the Operator service
     * @return If `opendal_{schema}_test` is on, construct a new Operator with given root.
     * Else, returns a `Empty` to represent no valid config for operator.
     */
    public static Optional<Operator> init(String schema) {
        final Properties properties = new Properties();

        try (InputStream is = Utils.class.getClassLoader().getResourceAsStream(ENV_NAME)) {
            properties.load(is);
        } catch (Exception ignore) {
        }
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        final String confPrefix = (CONF_PREFIX + schema).toLowerCase();
        final Map<String, String> conf = properties.entrySet().stream()
                .filter(Objects::nonNull)
                .filter(entry -> Optional.ofNullable(entry.getKey())
                        .map(Object::toString)
                        .orElse("")
                        .toLowerCase()
                        .startsWith(confPrefix))
                .collect(Collectors.toMap(
                        entry -> {
                            String key = entry.getKey().toString().toLowerCase();
                            return key.replace(confPrefix + "_", "");
                        },
                        entry -> Optional.ofNullable(entry.getValue())
                                .map(Object::toString)
                                .orElse(""),
                        (existing, replacement) -> existing));

        final String turnOnTest = conf.get(CONF_TURN_ON_TEST);
        if (!isTurnOn(turnOnTest)) {
            return Optional.empty();
        }
        if (!Boolean.parseBoolean(properties.getProperty(CONF_RANDOM_ROOT_FLAG))) {
            String root = conf.getOrDefault(CONF_ROOT, File.separator);
            if (!root.endsWith(File.separator)) {
                root = root + File.separator;
            }
            root = root + UUID.randomUUID() + File.separator;
            conf.put(CONF_ROOT, root);
        }
        Operator op = new Operator(schema, conf);
        return Optional.of(op);
    }

    /**
     * Determines if the given value is turn on.
     *
     * @param val the value to be checked
     * @return true if the value is "on" or "true", false otherwise
     */
    public static boolean isTurnOn(String val) {
        return "on".equalsIgnoreCase(val) || "true".equalsIgnoreCase(val);
    }

    public static byte[] generateBytes() {
        Random random = new Random();

        int size = random.nextInt(4 * 1024 * 1024) + 1;
        byte[] content = new byte[size];
        random.nextBytes(content);

        return content;
    }

    public static String generateRandomString() {
        Random random = new Random();

        int length = random.nextInt(256) + 1;
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < length; i++) {
            int randomChar = random.nextInt(26);
            stringBuilder.append((char) ('a' + randomChar));
        }

        return stringBuilder.toString();
    }
}
