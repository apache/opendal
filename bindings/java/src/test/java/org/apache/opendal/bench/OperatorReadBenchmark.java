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

package org.apache.opendal.bench;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorInputStream;

/**
 * A minimal Java-side benchmark to compare range read vs InputStream read.
 *
 * <p>This benchmark intentionally avoids adding new dependencies (e.g. JMH). It is designed for
 * quick iteration and relative comparison.
 *
 * <p>Environment variables:
 *
 * <ul>
 *   <li>{@code OPENDAL_BENCH_SCHEME}: service scheme, default {@code memory}
 *   <li>{@code OPENDAL_BENCH_CONFIG}: config map, format {@code k1=v1,k2=v2}, default empty
 *   <li>{@code OPENDAL_BENCH_SIZE_BYTES}: object size in bytes, default {@code 16777216} (16 MiB)
 *   <li>{@code OPENDAL_BENCH_BUFFER_BYTES}: stream buffer size, default {@code 8192}
 *   <li>{@code OPENDAL_BENCH_WARMUP}: warmup iterations, default {@code 3}
 *   <li>{@code OPENDAL_BENCH_ITERS}: measured iterations, default {@code 10}
 * </ul>
 */
public final class OperatorReadBenchmark {
    private OperatorReadBenchmark() {}

    public static void main(String[] args) throws Exception {
        final String scheme = getenvOrDefault("OPENDAL_BENCH_SCHEME", "memory");
        final Map<String, String> config =
                parseConfigMap(getenvOrDefault("OPENDAL_BENCH_CONFIG", ""));

        final int sizeBytes = parsePositiveInt(getenvOrDefault("OPENDAL_BENCH_SIZE_BYTES", "16777216"));
        final int bufferBytes = parsePositiveInt(getenvOrDefault("OPENDAL_BENCH_BUFFER_BYTES", "8192"));
        final int warmup = parseNonNegativeInt(getenvOrDefault("OPENDAL_BENCH_WARMUP", "3"));
        final int iters = parsePositiveInt(getenvOrDefault("OPENDAL_BENCH_ITERS", "10"));

        final String path = "bench.bin";

        try (final Operator op = Operator.of(scheme, config)) {
            final byte[] content = new byte[sizeBytes];
            new Random(0).nextBytes(content);
            op.write(path, content);

            System.out.println("OpenDAL Java benchmark");
            System.out.println("scheme=" + scheme + " configKeys=" + config.keySet());
            System.out.println("sizeBytes=" + sizeBytes + " bufferBytes=" + bufferBytes);
            System.out.println("warmup=" + warmup + " iters=" + iters);
            System.out.println();

            final NullOutputStream out = new NullOutputStream();
            long blackhole = 0;

            for (int i = 0; i < warmup; i++) {
                blackhole ^= benchRangeRead(op, path, sizeBytes, out);
                blackhole ^= benchInputStreamRead(op, path, bufferBytes, out);
            }

            final Result range = measure("readRange", iters, sizeBytes, () -> benchRangeRead(op, path, sizeBytes, out));
            final Result stream = measure("createInputStream", iters, sizeBytes, () -> benchInputStreamRead(op, path, bufferBytes, out));

            blackhole ^= range.blackhole;
            blackhole ^= stream.blackhole;

            printResult(range);
            printResult(stream);

            if (blackhole == 0x1234_5678_9ABC_DEF0L) {
                System.out.println("blackhole=" + blackhole);
            }
        }
    }

    private static long benchRangeRead(Operator op, String path, int sizeBytes, OutputStream out) throws IOException {
        final byte[] bytes = op.read(path, 0, sizeBytes);
        if (bytes.length != sizeBytes) {
            throw new IllegalStateException("short read: " + bytes.length + " != " + sizeBytes);
        }
        out.write(bytes);
        return ((long) bytes[0] << 32) ^ bytes[bytes.length - 1];
    }

    private static long benchInputStreamRead(Operator op, String path, int bufferBytes, OutputStream out)
            throws IOException {
        final byte[] buf = new byte[bufferBytes];
        long total = 0;
        long blackhole = 0;

        try (final OperatorInputStream in = op.createInputStream(path)) {
            while (true) {
                final int n = in.read(buf);
                if (n < 0) {
                    break;
                }
                if (n > 0) {
                    out.write(buf, 0, n);
                    total += n;
                    blackhole ^= ((long) buf[0] << 32) ^ buf[n - 1];
                }
            }
        }

        if (total == 0) {
            return blackhole;
        }
        return blackhole ^ total;
    }

    private static Result measure(String name, int iters, int bytesPerIter, ThrowingLongSupplier body) throws Exception {
        final long start = System.nanoTime();
        long blackhole = 0;
        for (int i = 0; i < iters; i++) {
            blackhole ^= body.getAsLong();
        }
        final long end = System.nanoTime();
        return new Result(name, iters, bytesPerIter, end - start, blackhole);
    }

    private static void printResult(Result r) {
        final double seconds = r.elapsedNanos / 1_000_000_000.0;
        final long totalBytes = (long) r.iters * (long) r.bytesPerIter;
        final double mib = totalBytes / (1024.0 * 1024.0);
        final double mibPerSec = mib / seconds;
        final double nsPerByte = (double) r.elapsedNanos / (double) totalBytes;

        System.out.println(
                r.name
                        + ": "
                        + String.format("%.2f", mibPerSec)
                        + " MiB/s"
                        + "  "
                        + String.format("%.2f", nsPerByte)
                        + " ns/B"
                        + "  "
                        + "(elapsed="
                        + String.format("%.3f", seconds)
                        + "s)");
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        final String v = System.getenv(key);
        if (v == null || v.trim().isEmpty()) {
            return defaultValue;
        }
        return v.trim();
    }

    private static int parsePositiveInt(String s) {
        final int v = Integer.parseInt(s);
        if (v <= 0) {
            throw new IllegalArgumentException("value must be positive: " + s);
        }
        return v;
    }

    private static int parseNonNegativeInt(String s) {
        final int v = Integer.parseInt(s);
        if (v < 0) {
            throw new IllegalArgumentException("value must be non-negative: " + s);
        }
        return v;
    }

    private static Map<String, String> parseConfigMap(String s) {
        final Map<String, String> map = new HashMap<String, String>();
        final String trimmed = s.trim();
        if (trimmed.isEmpty()) {
            return map;
        }
        final String[] pairs = trimmed.split(",");
        for (int i = 0; i < pairs.length; i++) {
            final String pair = pairs[i].trim();
            if (pair.isEmpty()) {
                continue;
            }
            final int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("invalid config pair: " + pair);
            }
            final String k = pair.substring(0, eq).trim();
            final String v = pair.substring(eq + 1).trim();
            map.put(k, v);
        }
        return map;
    }

    private interface ThrowingLongSupplier {
        long getAsLong() throws Exception;
    }

    private static final class Result {
        private final String name;
        private final int iters;
        private final int bytesPerIter;
        private final long elapsedNanos;
        private final long blackhole;

        private Result(String name, int iters, int bytesPerIter, long elapsedNanos, long blackhole) {
            this.name = name;
            this.iters = iters;
            this.bytesPerIter = bytesPerIter;
            this.elapsedNanos = elapsedNanos;
            this.blackhole = blackhole;
        }
    }

    private static final class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) {}

        @Override
        public void write(byte[] b, int off, int len) {}
    }
}

