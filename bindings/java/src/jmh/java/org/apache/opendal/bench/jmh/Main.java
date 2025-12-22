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

import java.util.Collection;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

public final class Main {
    private static final double BYTES_PER_MIB = 1024.0 * 1024.0;

    private Main() {}

    public static void main(String[] args) throws Exception {
        final CommandLineOptions cli = new CommandLineOptions(args);

        final ChainedOptionsBuilder builder = new OptionsBuilder().parent(cli);
        if (!hasVerbosityArg(args)) {
            builder.verbosity(VerboseMode.SILENT);
        }

        final Options opt = builder.build();
        final Collection<RunResult> results = new Runner(opt).run();

        System.out.println();
        System.out.println("Benchmark                                       MiB/s");
        for (final RunResult rr : results) {
            final double mibPerSec = toMibPerSec(rr);
            System.out.printf("%-45s %10.3f%n", rr.getParams().getBenchmark(), mibPerSec);
        }
    }

    private static double toMibPerSec(RunResult rr) {
        final BenchmarkParams params = rr.getParams();
        final String size = params.getParam("sizeBytes");
        if (size == null) {
            throw new IllegalStateException("missing sizeBytes parameter for " + params.getBenchmark());
        }
        final double mibPerOp = Long.parseLong(size) / BYTES_PER_MIB;
        final double opsPerSec = rr.getPrimaryResult().getScore();
        return opsPerSec * mibPerOp;
    }

    private static boolean hasVerbosityArg(String[] args) {
        for (int i = 0; i < args.length; i++) {
            final String a = args[i];
            if ("-v".equals(a) || "--verbosity".equals(a)) {
                return true;
            }
            if (a.startsWith("-v") && a.length() > 2) {
                return true;
            }
            if (a.startsWith("--verbosity=")) {
                return true;
            }
        }
        return false;
    }
}
