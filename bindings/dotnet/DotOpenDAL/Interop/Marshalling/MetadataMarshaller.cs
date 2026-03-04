

using System.Runtime.CompilerServices;
using DotOpenDAL.Interop.NativeObject;

namespace DotOpenDAL.Interop.Marshalling;

/// <summary>
/// Converts native metadata payloads into managed <see cref="Metadata"/> instances.
/// </summary>
internal static class MetadataMarshaller
{
    /// <summary>
    /// Reads a native metadata pointer and converts it to managed metadata.
    /// </summary>
    /// <param name="ptr">Pointer to a native <c>opendal_metadata</c> payload.</param>
    /// <returns>A managed <see cref="Metadata"/> instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the pointer is null.</exception>
    internal static unsafe Metadata ToMetadata(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("stat returned null metadata pointer");
        }

        var payload = Unsafe.Read<OpenDALMetadata>((void*)ptr);
        return ToMetadata(payload);
    }

    /// <summary>
    /// Converts a native metadata payload structure into managed metadata.
    /// </summary>
    /// <param name="payload">Native metadata payload copied from unmanaged memory.</param>
    /// <returns>A managed <see cref="Metadata"/> instance.</returns>
    internal static Metadata ToMetadata(OpenDALMetadata payload)
    {
        DateTimeOffset? lastModified = null;
        if (payload.LastModifiedHasValue != 0)
        {
            lastModified = DateTimeOffset.FromUnixTimeSeconds(payload.LastModifiedSecond)
                .AddTicks(payload.LastModifiedNanosecond / 100);
        }

        var mode = payload.Mode switch
        {
            0 => EntryMode.File,
            1 => EntryMode.Dir,
            _ => EntryMode.Unknown,
        };

        return new Metadata(
            mode,
            payload.ContentLength,
            Utilities.ReadNullableUtf8(payload.ContentDisposition),
            Utilities.ReadNullableUtf8(payload.ContentMd5),
            Utilities.ReadNullableUtf8(payload.ContentType),
            Utilities.ReadNullableUtf8(payload.ContentEncoding),
            Utilities.ReadNullableUtf8(payload.CacheControl),
            Utilities.ReadNullableUtf8(payload.ETag),
            lastModified,
            Utilities.ReadNullableUtf8(payload.Version)
        );
    }
}