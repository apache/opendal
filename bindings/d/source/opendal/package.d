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

module opendal;

public import opendal.operator;

version (unittest)
{
	@("Test basic Operator creation")
	@safe unittest
	{
		/* Initialize a operator for "memory" backend, with no options */
		OperatorOptions options = new OperatorOptions();
		Operator op = Operator("memory", options);

		/* Prepare some data to be written */
		string data = "this_string_length_is_24";

		/* Write this into path "/testpath" */
		op.write("/testpath", cast(ubyte[])data.dup);

		/* We can read it out, make sure the data is the same */
		auto read_bytes = op.read("/testpath");
		assert(read_bytes.length == 24);
		assert(cast(string)read_bytes.idup == data);
	}

	@("Benchmark parallel and normal functions")
	@safe unittest
	{
		import std.exception: assertNotThrown;
		import std.file: tempDir;
		import std.path: buildPath;
		import std.datetime.stopwatch: StopWatch;
		import std.stdio: writeln;

		auto options = new OperatorOptions();
		options.set("root", tempDir);
		auto op = Operator("fs", options, true);

		auto testPath = buildPath(tempDir, "benchmark_test.txt");
		auto testData = cast(ubyte[])"Benchmarking OpenDAL async and normal functions".dup;

		// Benchmark write operations
		StopWatch sw;

		sw.start();
		assertNotThrown(op.write(testPath, testData));
		sw.stop();
		auto normalWriteTime = sw.peek();

		sw.reset();
		sw.start();
		assertNotThrown(op.writeParallel(testPath, testData));
		sw.stop();
		auto parallelWriteTime = sw.peek();

		// Benchmark read operations
		sw.reset();
		sw.start();
		auto normalReadData = op.read(testPath);
		sw.stop();
		auto normalReadTime = sw.peek();

		sw.reset();
		sw.start();
		auto parallelReadData = op.readParallel(testPath);
		sw.stop();
		auto parallelReadTime = sw.peek();

		// Benchmark list operations
		sw.reset();
		sw.start();
		op.list(tempDir);
		sw.stop();
		auto normalListTime = sw.peek();

		sw.reset();
		sw.start();
		op.listParallel(tempDir);
		sw.stop();
		auto parallelListTime = sw.peek();

		// Print benchmark results
		writeln("Write benchmark:");
		writeln("  Normal:   ", normalWriteTime);
		writeln("  Parallel: ", parallelWriteTime);

		writeln("Read benchmark:");
		writeln("  Normal:   ", normalReadTime);
		writeln("  Parallel: ", parallelReadTime);

		writeln("List benchmark:");
		writeln("  Normal:   ", normalListTime);
		writeln("  Parallel: ", parallelListTime);

		// Verify data integrity
		assert(normalReadData == testData);
		assert(parallelReadData == testData);

		// Clean up
		op.remove(testPath);
		assert(!op.exists(testPath));
	}

}
