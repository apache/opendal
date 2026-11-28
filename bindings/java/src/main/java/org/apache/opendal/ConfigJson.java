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

import java.util.Map;
import java.util.TreeMap;

/**
 * Encodes map-valued {@link ServiceConfig} fields as the JSON object strings that core parses.
 *
 * <p>The binding has no runtime dependencies and the JDK has no JSON encoder, so this class
 * implements the string escaping that RFC 8259 requires and nothing else.
 */
final class ConfigJson {
    private ConfigJson() {}

    /**
     * Encode a string map as a JSON object with keys sorted by {@link String#compareTo}.
     *
     * <p>A null value becomes JSON {@code null}, which core rejects with {@code ConfigInvalid}. A
     * null key throws {@link NullPointerException}, because JSON has no null key.
     */
    static String encodeObject(Map<String, String> map) {
        final StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : new TreeMap<>(map).entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            appendString(sb, entry.getKey());
            sb.append(':');
            if (entry.getValue() == null) {
                sb.append("null");
            } else {
                appendString(sb, entry.getValue());
            }
        }
        return sb.append('}').toString();
    }

    private static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }
}
