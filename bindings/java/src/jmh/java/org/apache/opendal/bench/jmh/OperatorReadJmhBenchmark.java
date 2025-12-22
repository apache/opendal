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

package org.apache.opendal.bench.jmh;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorInputStream;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class OperatorReadJmhBenchmark {
    private static final double BYTES_PER_MIB = 1024.0 * 1024.0;

    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class Counters {
        public double mibPerSec;

        @Setup(Level.Iteration)
        public void reset() {
            mibPerSec = 0.0;
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkState {
        @Param({"16777216"})
        public int sizeBytes;

        @Param({"8192"})
        public int bufferBytes;

        private Operator operator;
        private String path;
        private byte[] buf;
        private OutputStream out;

        @Setup(Level.Trial)
        public void setup() {
            final Map<String, String> config = new HashMap<String, String>();
            this.operator = Operator.of("memory", config);
            this.path = "bench.bin";

            final byte[] content = new byte[sizeBytes];
            new Random(0).nextBytes(content);
            this.operator.write(path, content);

            this.buf = new byte[bufferBytes];
            this.out = new NullOutputStream();
        }

        @TearDown(Level.Trial)
        public void teardown() {
            if (this.operator != null) {
                this.operator.close();
            }
        }
    }

    @Benchmark
    public void readRange(BenchmarkState state, Counters counters, Blackhole bh) throws IOException {
        final byte[] bytes = state.operator.read(state.path, 0, state.sizeBytes);
        if (bytes.length != state.sizeBytes) {
            throw new IllegalStateException("short read: " + bytes.length + " != " + state.sizeBytes);
        }
        state.out.write(bytes);
        counters.mibPerSec += bytes.length / BYTES_PER_MIB;
        bh.consume(bytes.length);
    }

    @Benchmark
    public void createInputStream(BenchmarkState state, Counters counters, Blackhole bh) throws IOException {
        long total = 0;
        long acc = 0;

        try (final OperatorInputStream in = state.operator.createInputStream(state.path)) {
            while (true) {
                final int n = in.read(state.buf);
                if (n < 0) {
                    break;
                }
                if (n == 0) {
                    continue;
                }
                state.out.write(state.buf, 0, n);
                total += n;
                acc ^= ((long) state.buf[0] << 32) ^ (state.buf[n - 1] & 0xFFL);
            }
        }

        if (total != state.sizeBytes) {
            throw new IllegalStateException("short read: " + total + " != " + state.sizeBytes);
        }
        counters.mibPerSec += total / BYTES_PER_MIB;
        bh.consume(acc);
    }

    private static final class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) {}

        @Override
        public void write(byte[] b, int off, int len) {}
    }
}
