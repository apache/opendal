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
import static org.assertj.core.api.Assertions.entry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;

public class ServiceConfigMapFieldTest {
    private static ServiceConfig.S3 s3WithTags(Map<String, String> tags) {
        return ServiceConfig.S3
                .builder()
                .bucket("b")
                .region("us-east-1")
                .assumeRoleSessionTags(tags)
                .build();
    }

    private static Map<String, String> trickyTags() {
        final Map<String, String> tags = new LinkedHashMap<>();
        tags.put("q\"", "b\\");
        tags.put("nl", "a\nb");
        tags.put("ctl", "\u0001");
        tags.put("\u00fc", "\u00df");
        return tags;
    }

    @Test
    void s3SessionTagsBecomeOneJsonEntry() {
        final Map<String, String> map =
                s3WithTags(Collections.singletonMap("Team", "x")).configMap();
        assertThat(map).containsEntry("assume_role_session_tags", "{\"Team\":\"x\"}");
        assertThat(map).doesNotContainKey("Team");
    }

    @Test
    void mapEntriesDoNotBecomeServiceOptions() {
        final Map<String, String> map =
                s3WithTags(Collections.singletonMap("Region", "eu-west-1")).configMap();
        assertThat(map).containsEntry("region", "us-east-1");
        assertThat(map).containsEntry("assume_role_session_tags", "{\"Region\":\"eu-west-1\"}");
        assertThat(map).doesNotContainKey("Region");
    }

    @Test
    void sessionTagsRoundTripAlongsideRegion() throws Exception {
        final Map<String, String> map =
                s3WithTags(Collections.singletonMap("Region", "eu-west-1")).configMap();
        final Map<String, String> decoded = new ObjectMapper()
                .readValue(map.get("assume_role_session_tags"), new TypeReference<Map<String, String>>() {});
        assertThat(decoded).containsExactly(entry("Region", "eu-west-1"));
        assertThat(map).containsEntry("region", "us-east-1");
    }

    @Test
    void keysAreSortedAndKeepTheirCase() {
        final Map<String, String> tags = new LinkedHashMap<>();
        tags.put("b", "2");
        tags.put("a", "1");
        tags.put("Fs.DefaultFS", "x");
        assertThat(s3WithTags(tags).configMap())
                .containsEntry("assume_role_session_tags", "{\"Fs.DefaultFS\":\"x\",\"a\":\"1\",\"b\":\"2\"}");
    }

    @Test
    void jsonEscapesQuotesBackslashesAndControlCharacters() {
        assertThat(s3WithTags(trickyTags()).configMap())
                .containsEntry(
                        "assume_role_session_tags",
                        "{\"ctl\":\"\\u0001\",\"nl\":\"a\\nb\",\"q\\\"\":\"b\\\\\",\"\u00fc\":\"\u00df\"}");
    }

    @Test
    void emptyMapIsExplicitAndNullIsOmitted() {
        assertThat(s3WithTags(Collections.emptyMap()).configMap()).containsEntry("assume_role_session_tags", "{}");
        assertThat(ServiceConfig.S3.builder().bucket("b").build().configMap())
                .doesNotContainKey("assume_role_session_tags");
    }

    @Test
    void hdfsNativeOptionsBecomeOneJsonEntry() {
        // The default native library does not enable services-hdfs-native, so check the map only.
        final Map<String, String> map = ServiceConfig.HdfsNative.builder()
                .nameNode("default")
                .options(Collections.singletonMap("fs.defaultFS", "hdfs://ns"))
                .build()
                .configMap();
        assertThat(map).containsEntry("options", "{\"fs.defaultFS\":\"hdfs://ns\"}");
        assertThat(map).doesNotContainKey("fs.defaultFS");
    }

    @Test
    void coreAcceptsEncodedTags() {
        final ServiceConfig.S3 config = s3WithTags(trickyTags());
        try (Operator op = Operator.of(config)) {
            assertThat(op.info.scheme).isEqualTo("s3");
        }
        try (AsyncOperator op = AsyncOperator.of(config)) {
            assertThat(op.info.scheme).isEqualTo("s3");
        }
    }

    @Test
    void coreRejectsNullTagValue() {
        final ServiceConfig.S3 config = s3WithTags(Collections.singletonMap("Team", null));
        assertThatThrownBy(() -> Operator.of(config))
                .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid))
                .hasMessageContaining("assume_role_session_tags");
    }

    @Test
    void nullTagKeyIsRejected() {
        final Map<String, String> tags = new HashMap<>();
        tags.put(null, "x");
        final ServiceConfig.S3 config = s3WithTags(tags);
        assertThatThrownBy(config::configMap).isInstanceOf(NullPointerException.class);
    }
}
