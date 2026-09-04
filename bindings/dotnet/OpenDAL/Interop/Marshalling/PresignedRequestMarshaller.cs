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

using System.Runtime.CompilerServices;
using OpenDAL.Interop.NativeObject;

namespace OpenDAL.Interop.Marshalling;

internal static class PresignedRequestMarshaller
{
    internal static unsafe PresignedRequest ToPresignedRequest(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("presign returned null request pointer");
        }

        var payload = Unsafe.Read<OpenDALPresignedRequest>((void*)ptr);
        var method = Utilities.ReadUtf8(payload.Method);
        var uri = Utilities.ReadUtf8(payload.Uri);
        var headers = Utilities.ReadStringPairs(
            payload.HeadersKeys,
            payload.HeadersValues,
            payload.HeadersLen,
            StringComparer.OrdinalIgnoreCase
        );
        return new PresignedRequest(method, uri, headers);
    }
}
