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

module opendal.operator;

import std.string: toStringz;
import std.exception: enforce;
import std.conv: to;
import std.parallelism: task, TaskPool;

/// OpenDAL-C binding for D. (unsafe/@system)
private import opendal.opendal_c;

struct Operator
{
    private opendal_operator* op;
    private TaskPool taskPool;
    private bool enabledParallelism;

    this(string scheme, OperatorOptions options, bool useParallel = false) @trusted
    {
        auto error = opendal_operator_new(scheme.toStringz, options.options, &op);
        enforce(error is null, "Error in Operator");
        enforce(op !is null, "Failed to create Operator");
        enabledParallelism = useParallel;

        if (enabledParallelism)
            taskPool = new TaskPool();
    }

    void write(string path, ubyte[] data) @trusted
    {
        opendal_bytes bytes = opendal_bytes(data.ptr, data.length, data.length);
        auto error = opendal_operator_write(op, path.toStringz, &bytes);
        enforce(error is null, "Error writing data");
    }

    void writeParallel(string path, ubyte[] data) @safe
    {
        auto t = task!((Operator* op, string p, ubyte[] d) { op.write(p, d); })(&this, path, data);
        taskPool.put(t);
        t.yieldForce();
    }

    ubyte[] readParallel(string path) @trusted
    {
        auto t = task!((Operator* op, string p) { return op.read(p); })(&this, path);
        taskPool.put(t);
        return t.yieldForce();
    }

    Entry[] listParallel(string path) @trusted
    {
        auto t = task!((Operator* op, string p) { return op.list(p); })(&this, path);
        taskPool.put(t);
        return t.yieldForce();
    }

    ubyte[] read(string path) @trusted
    {
        opendal_bytes data;
        auto error = opendal_operator_read(op, path.toStringz, &data);
        enforce(error is null, "Error reading data");
        scope (exit)
            opendal_bytes_free(&data);
        return data.data[0 .. data.len].dup;
    }

    void remove(string path) @trusted
    {
        auto error = opendal_operator_delete(op, path.toStringz);
        enforce(error is null, "Error deleting object");
    }

    bool exists(string path) @trusted
    {
        bool is_exist = false;
        auto error = opendal_operator_is_exist(op, path.toStringz, &is_exist);
        enforce(error is null, "Error checking existence");
        return is_exist;
    }

    Metadata stat(string path) @trusted
    {
        opendal_metadata* meta = null;
        auto error = opendal_operator_stat(op, path.toStringz, &meta);
        enforce(error is null, "Error getting metadata");
        return Metadata(meta);
    }

    Entry[] list(string path) @trusted
    {
        opendal_lister* lister = null;
        auto error = opendal_operator_list(op, path.toStringz, &lister);
        enforce(error is null, "Error listing objects");
        scope (exit)
            opendal_lister_free(lister);

        Entry[] entries;
        while (true)
        {
            opendal_entry* entry = null;
            auto next_error = opendal_lister_next(lister, &entry);
            enforce(next_error is null, "Error getting next entry");
            if (entry is null)
                break;
            entries ~= Entry(entry);
        }
        return entries;
    }

    void createDir(string path) @trusted
    {
        auto error = opendal_operator_create_dir(op, path.toStringz);
        enforce(error is null, "Error creating directory");
    }

    void rename(string src, string dest) @trusted
    {
        auto error = opendal_operator_rename(op, src.toStringz, dest.toStringz);
        enforce(error is null, "Error renaming object");
    }

    void copy(string src, string dest) @trusted
    {
        auto error = opendal_operator_copy(op, src.toStringz, dest.toStringz);
        enforce(error is null, "Error copying object");
    }

    ~this() @trusted
    {
        if (op !is null)
            opendal_operator_free(op);
        if (enabledParallelism)
            taskPool.stop();
    }
}

class OperatorOptions
{
    private opendal_operator_options* options;

    this() @trusted
    {
        options = opendal_operator_options_new();
    }

    void set(string key, string value) @trusted
    {
        opendal_operator_options_set(options, key.toStringz, value.toStringz);
    }

    ~this() @trusted
    {
        if (options !is null)
            opendal_operator_options_free(options);
    }
}

struct Metadata
{
    private opendal_metadata* meta;

    this(scope opendal_metadata* meta) @trusted pure
    {
        this.meta = meta;
    }

    ulong contentLength() @trusted
    {
        return opendal_metadata_content_length(meta);
    }

    bool isFile() @trusted
    {
        return opendal_metadata_is_file(meta);
    }

    bool isDir() @trusted
    {
        return opendal_metadata_is_dir(meta);
    }

    long lastModified() @trusted
    {
        return opendal_metadata_last_modified_ms(meta);
    }

    ~this() @trusted
    {
        if (meta !is null)
            opendal_metadata_free(meta);
    }
}

struct Entry
{
    private opendal_entry* entry;

    this(opendal_entry* entry) pure @nogc
    {
        this.entry = entry;
    }

    string path()
    {
        return to!string(opendal_entry_path(entry));
    }

    string name()
    {
        return to!string(opendal_entry_name(entry));
    }

    ~this()
    {
        if (entry !is null)
            opendal_entry_free(entry);
    }
}
