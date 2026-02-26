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

using System;
using System.Runtime.InteropServices;

namespace DotOpenDAL;

public partial class BlockingOperator
{
    public IntPtr Op { get; }

    public BlockingOperator()
    {
        Op = blocking_operator_construct("memory");
    }

    public void Write(string path, string content)
    {
        blocking_operator_write(Op, path, content);
    }

    public string Read(string path)
    {
        return blocking_operator_read(Op, path);
    }

    [LibraryImport("opendal_dotnet", EntryPoint = "blocking_operator_construct", StringMarshalling = StringMarshalling.Utf8)]
    private static partial IntPtr blocking_operator_construct(string scheme);

    [LibraryImport("opendal_dotnet", EntryPoint = "blocking_operator_write", StringMarshalling = StringMarshalling.Utf8)]
    private static partial void blocking_operator_write(IntPtr op, string path, string content);

    [LibraryImport("opendal_dotnet", EntryPoint = "blocking_operator_read", StringMarshalling = StringMarshalling.Utf8)]
    private static partial string blocking_operator_read(IntPtr op, string path);
}
