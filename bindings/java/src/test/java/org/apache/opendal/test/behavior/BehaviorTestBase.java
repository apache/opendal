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

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class BehaviorTestBase {
    @RegisterExtension
    public static final BehaviorExtension behaviorExtension = new BehaviorExtension();

    @BeforeAll
    public static void assume() {
        assumeTrue(behaviorExtension.asyncOperator != null);
        assumeTrue(behaviorExtension.operator != null);
    }

    protected AsyncOperator asyncOp() {
        return behaviorExtension.asyncOperator;
    }

    protected Operator op() {
        return behaviorExtension.operator;
    }

    protected String scheme() {
        return behaviorExtension.scheme;
    }

    /**
     * Generates a byte array of random content.
     */
    public static byte[] generateBytes() {
        final Random random = new Random();
        final int size = random.nextInt(4 * 1024 * 1024) + 1;
        final byte[] content = new byte[size];
        random.nextBytes(content);
        return content;
    }

    /**
     * Generates a byte array of random content with a specific size.
     */
    public static byte[] generateBytes(int size) {
        final Random random = new Random();
        final byte[] content = new byte[size];
        random.nextBytes(content);
        return content;
    }

    /**
     * Calculate SHA256 digest of input bytes
     *
     * @param input input bytes
     * @return SHA256 digest string
     */
    public static String sha256Digest(final byte[] input) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final byte[] hash = digest.digest(input);
        final StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            final String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
