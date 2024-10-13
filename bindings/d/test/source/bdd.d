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
module bdd;

import opendal;
import std.stdio: writeln;

class OperatorContext
{
    Operator op;

    this() @trusted
    {
        auto options = new OperatorOptions();
        op = Operator("memory", options);
    }
}

class WriteScenario
{
    OperatorContext context;
    string data;
    string path;

    this(OperatorContext context) @trusted
    {
        this.context = context;
    }

    WriteScenario givenSomeData(string data)
    {
        this.data = data;
        return this;
    }

    WriteScenario whenWritingToPath(string path)
    {
        this.path = path;
        context.op.write(path, cast(ubyte[])data.dup);
        return this;
    }

    void thenDataShouldBeReadable()
    {
        auto read_bytes = context.op.read(path);
        assert(read_bytes.length == data.length, "Read data length does not match written data length");
        assert(cast(string)read_bytes.idup == data, "Read data does not match written data");
    }
}

void main() @safe
{
    auto context = new OperatorContext();

    describe("Operator memory backend", {
        it("should write and read data correctly", {
            new WriteScenario(context)
            .givenSomeData("this_string_length_is_24")
            .whenWritingToPath("/testpath")
            .thenDataShouldBeReadable();
        });

        it("should print the read data", {
            auto read_bytes = context.op.read("/testpath");
            writeln(cast(string)read_bytes.idup);
        });
    });
}

void describe(string description, void delegate() tests) @trusted
{
    writeln("Describe: ", description);
    tests();
}

void it(string description, void delegate() test)
{
    writeln("  It ", description);
    test();
    writeln("    - Passed");
}
