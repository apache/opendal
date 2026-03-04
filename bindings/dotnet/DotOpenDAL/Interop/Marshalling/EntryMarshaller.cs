

using System.Runtime.CompilerServices;
using DotOpenDAL.Interop.NativeObject;

namespace DotOpenDAL.Interop.Marshalling;

/// <summary>
/// Converts native entry list payloads into managed <see cref="Entry"/> collections.
/// </summary>
internal static class EntryMarshaller
{
    /// <summary>
    /// Reads a native entry list pointer and converts it into managed entries.
    /// </summary>
    /// <param name="ptr">Pointer to a native <c>opendal_entry_list</c> payload.</param>
    /// <returns>A read-only collection of managed <see cref="Entry"/> values.</returns>
    /// <exception cref="InvalidOperationException">Thrown when native list size exceeds <see cref="int.MaxValue"/>.</exception>
    internal static unsafe IReadOnlyList<Entry> ToEntries(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return Array.Empty<Entry>();
        }

        var payload = Unsafe.Read<OpenDALEntryList>((void*)ptr);

        if (payload.Len > int.MaxValue)
        {
            throw new InvalidOperationException("Entry list too large");
        }

        var count = (int)payload.Len;
        var results = new List<Entry>(count);

        if (payload.Entries == IntPtr.Zero)
        {
            return results;
        }

        var entryPointers = new ReadOnlySpan<IntPtr>((void*)payload.Entries, count);
        for (var index = 0; index < count; index++)
        {
            var entryPtr = entryPointers[index];
            if (entryPtr == IntPtr.Zero)
            {
                continue;
            }

            var entryPayload = Unsafe.Read<OpenDALEntry>((void*)entryPtr);
            if (entryPayload.Metadata == IntPtr.Zero)
            {
                continue;
            }

            var path = Utilities.ReadUtf8(entryPayload.Path);
            var metadata = MetadataMarshaller.ToMetadata(entryPayload.Metadata);
            results.Add(new Entry(path, metadata));
        }

        return results;
    }
}