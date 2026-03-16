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
using DotOpenDAL.Interop.NativeObject;

namespace DotOpenDAL.Interop.Marshalling;

/// <summary>
/// Converts native operator info payloads into managed <see cref="OperatorInfo"/> instances.
/// </summary>
internal static class OperatorInfoMarshaller
{
    /// <summary>
    /// Reads a native operator info pointer and converts it to managed operator info.
    /// </summary>
    /// <param name="ptr">Pointer to a native <c>opendal_operator_info</c> payload.</param>
    /// <returns>A managed <see cref="OperatorInfo"/> instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the pointer is null.</exception>
    internal static unsafe OperatorInfo ToOperatorInfo(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("operator_info_get returned null pointer");
        }

        var payload = Unsafe.Read<OpenDALOperatorInfo>((void*)ptr);
        return ToOperatorInfo(payload);
    }

    /// <summary>
    /// Converts a native operator info payload structure into managed operator info.
    /// </summary>
    /// <param name="payload">Native operator info payload copied from unmanaged memory.</param>
    /// <returns>A managed <see cref="OperatorInfo"/> instance.</returns>
    internal static OperatorInfo ToOperatorInfo(OpenDALOperatorInfo payload)
    {
        return new OperatorInfo(
            Utilities.ReadUtf8(payload.Scheme),
            Utilities.ReadUtf8(payload.Root),
            Utilities.ReadUtf8(payload.Name),
            new Capability(payload.FullCapability),
            new Capability(payload.NativeCapability)
        );
    }
}