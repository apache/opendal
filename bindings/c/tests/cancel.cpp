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

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
extern "C" {
#include "opendal.h"
}

class OpendalCancelTest : public ::testing::Test {
protected:
    const opendal_operator* op = nullptr;

    void SetUp() override
    {
        opendal_operator_options* options = opendal_operator_options_new();
        opendal_operator_options_set(options, "root", "/cancel_test");

        opendal_operator* created = nullptr;
        opendal_error* error = opendal_operator_new("memory", options, &created);
        ASSERT_EQ(error, nullptr);
        ASSERT_NE(created, nullptr);

        this->op = created;
        opendal_operator_options_free(options);
    }

    void TearDown() override
    {
        opendal_operator_free(this->op);
        this->op = nullptr;
    }
};

TEST_F(OpendalCancelTest, TokenLifecycleAndAbiStability)
{
    // Verify OPENDAL_CANCELLED is appended at discriminant 13, keeping 0..=12 unchanged.
    EXPECT_EQ(static_cast<int>(OPENDAL_UNEXPECTED), 0);
    EXPECT_EQ(static_cast<int>(OPENDAL_CONFLICT), 12);
    EXPECT_EQ(static_cast<int>(OPENDAL_CANCELLED), 13);

    // NULL-safe operations.
    opendal_cancel_token_cancel(nullptr);
    EXPECT_FALSE(opendal_cancel_token_is_cancelled(nullptr));
    opendal_cancel_token_free(nullptr);

    opendal_cancel_token* token = opendal_cancel_token_new();
    ASSERT_NE(token, nullptr);
    EXPECT_FALSE(opendal_cancel_token_is_cancelled(token));

    opendal_cancel_token_cancel(token);
    EXPECT_TRUE(opendal_cancel_token_is_cancelled(token));

    // Idempotent repeated cancel.
    opendal_cancel_token_cancel(token);
    EXPECT_TRUE(opendal_cancel_token_is_cancelled(token));

    opendal_cancel_token_free(token);
}

TEST_F(OpendalCancelTest, NullTokenNormalOperations)
{
    const std::string payload = "hello cancel api";
    opendal_bytes data = {
        .data = reinterpret_cast<uint8_t*>(const_cast<char*>(payload.data())),
        .len = payload.size(),
        .capacity = payload.size(),
    };

    // Write with NULL cancel token and metadata out.
    opendal_metadata* write_meta = nullptr;
    opendal_error* err = opendal_operator_write_with_cancel(
        this->op, "a.txt", &data, nullptr, nullptr, &write_meta);
    ASSERT_EQ(err, nullptr);
    ASSERT_NE(write_meta, nullptr);
    opendal_metadata_free(write_meta);

    // Check with NULL cancel token.
    err = opendal_operator_check_with_cancel(this->op, nullptr);
    ASSERT_EQ(err, nullptr);

    // Exists with NULL cancel token.
    bool exists = false;
    err = opendal_operator_exists_with_cancel(this->op, "a.txt", nullptr, &exists);
    ASSERT_EQ(err, nullptr);
    EXPECT_TRUE(exists);

    // Stat with NULL cancel token.
    opendal_metadata* stat_meta = nullptr;
    err = opendal_operator_stat_with_cancel(this->op, "a.txt", nullptr, nullptr, &stat_meta);
    ASSERT_EQ(err, nullptr);
    ASSERT_NE(stat_meta, nullptr);
    EXPECT_EQ(opendal_metadata_content_length(stat_meta), payload.size());
    opendal_metadata_free(stat_meta);

    // Read with NULL cancel token.
    opendal_bytes read_bytes = { nullptr, 0, 0 };
    err = opendal_operator_read_with_cancel(this->op, "a.txt", nullptr, nullptr, &read_bytes);
    ASSERT_EQ(err, nullptr);
    ASSERT_EQ(read_bytes.len, payload.size());
    EXPECT_EQ(std::memcmp(read_bytes.data, payload.data(), payload.size()), 0);
    opendal_bytes_free(&read_bytes);

    // Reader open + seek + read with NULL cancel token.
    opendal_reader* reader = nullptr;
    err = opendal_operator_reader_with_cancel(this->op, "a.txt", nullptr, nullptr, &reader);
    ASSERT_EQ(err, nullptr);
    ASSERT_NE(reader, nullptr);

    uint64_t pos = 0;
    err = opendal_reader_seek_with_cancel(reader, 6, OPENDAL_SEEK_SET, nullptr, &pos);
    ASSERT_EQ(err, nullptr);
    EXPECT_EQ(pos, 6u);

    uint8_t rbuf[32] = { 0 };
    uintptr_t nread = 0;
    err = opendal_reader_read_with_cancel(reader, rbuf, sizeof(rbuf), nullptr, &nread);
    ASSERT_EQ(err, nullptr);
    EXPECT_EQ(nread, 10u);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(rbuf), nread), "cancel api");
    opendal_reader_free(reader);

    // Writer open + write + close with NULL cancel token.
    opendal_writer* writer = nullptr;
    err = opendal_operator_writer_with_cancel(this->op, "b.txt", nullptr, nullptr, &writer);
    ASSERT_EQ(err, nullptr);
    ASSERT_NE(writer, nullptr);

    uintptr_t nwritten = 0;
    err = opendal_writer_write_with_cancel(writer, &data, nullptr, &nwritten);
    ASSERT_EQ(err, nullptr);
    EXPECT_EQ(nwritten, payload.size());

    opendal_metadata* close_meta = nullptr;
    err = opendal_writer_close_with_cancel(writer, nullptr, &close_meta);
    ASSERT_EQ(err, nullptr);
    ASSERT_NE(close_meta, nullptr);
    opendal_metadata_free(close_meta);
    opendal_writer_free(writer);

    // Create dir + list + lister_next with NULL cancel token.
    err = opendal_operator_create_dir_with_cancel(this->op, "dir/", nullptr);
    ASSERT_EQ(err, nullptr);

    opendal_lister* lister = nullptr;
    err = opendal_operator_list_with_cancel(this->op, "/", nullptr, nullptr, &lister);
    ASSERT_EQ(err, nullptr);
    ASSERT_NE(lister, nullptr);

    int entry_count = 0;
    while (true) {
        opendal_entry* entry = nullptr;
        err = opendal_lister_next_with_cancel(lister, nullptr, &entry);
        ASSERT_EQ(err, nullptr);
        if (entry == nullptr) {
            break;
        }
        entry_count++;
        opendal_entry_free(entry);
    }
    EXPECT_GE(entry_count, 2);
    opendal_lister_free(lister);

    // Deleter open + delete + delete_many + flush with NULL cancel token.
    opendal_deleter* deleter = nullptr;
    err = opendal_operator_deleter_with_cancel(this->op, nullptr, &deleter);
    ASSERT_EQ(err, nullptr);
    ASSERT_NE(deleter, nullptr);

    err = opendal_deleter_delete_with_cancel(deleter, "a.txt", nullptr, nullptr);
    ASSERT_EQ(err, nullptr);

    const char* paths[] = { "b.txt" };
    err = opendal_deleter_delete_many_with_cancel(deleter, paths, 1, nullptr);
    ASSERT_EQ(err, nullptr);

    err = opendal_deleter_flush_with_cancel(deleter, nullptr);
    ASSERT_EQ(err, nullptr);
    opendal_deleter_free(deleter);

    // Delete with NULL cancel token.
    err = opendal_operator_delete_with_cancel(this->op, "dir/", nullptr, nullptr);
    ASSERT_EQ(err, nullptr);
}

TEST_F(OpendalCancelTest, PreCancelledTokenReturnsCancelledAndZeroesOutputs)
{
    const std::string payload = "seed";
    opendal_bytes seed = {
        .data = reinterpret_cast<uint8_t*>(const_cast<char*>(payload.data())),
        .len = payload.size(),
        .capacity = payload.size(),
    };
    ASSERT_EQ(opendal_operator_write(this->op, "seed.txt", &seed), nullptr);

    opendal_cancel_token* token = opendal_cancel_token_new();
    ASSERT_NE(token, nullptr);
    opendal_cancel_token_cancel(token);

    auto expect_cancelled = [](opendal_error* err) {
        ASSERT_NE(err, nullptr);
        EXPECT_EQ(err->code, OPENDAL_CANCELLED);
        EXPECT_NE(err->message.data, nullptr);
        EXPECT_GT(err->message.len, 0u);
        opendal_error_free(err);
    };

    // 1. read
    opendal_bytes read_bytes = { reinterpret_cast<uint8_t*>(0x1), 99, 99 };
    expect_cancelled(
        opendal_operator_read_with_cancel(this->op, "seed.txt", nullptr, token, &read_bytes));
    EXPECT_EQ(read_bytes.data, nullptr);
    EXPECT_EQ(read_bytes.len, 0u);

    // 2. write
    opendal_metadata* write_meta = reinterpret_cast<opendal_metadata*>(0x1);
    expect_cancelled(opendal_operator_write_with_cancel(
        this->op, "out.txt", &seed, nullptr, token, &write_meta));
    EXPECT_EQ(write_meta, nullptr);

    // 3. stat
    opendal_metadata* stat_meta = reinterpret_cast<opendal_metadata*>(0x1);
    expect_cancelled(
        opendal_operator_stat_with_cancel(this->op, "seed.txt", nullptr, token, &stat_meta));
    EXPECT_EQ(stat_meta, nullptr);

    // 4. exists
    bool exists = true;
    expect_cancelled(opendal_operator_exists_with_cancel(this->op, "seed.txt", token, &exists));
    EXPECT_FALSE(exists);

    // 5. delete
    expect_cancelled(opendal_operator_delete_with_cancel(this->op, "seed.txt", nullptr, token));

    // 6. create_dir
    expect_cancelled(opendal_operator_create_dir_with_cancel(this->op, "cdir/", token));

    // 7. rename
    expect_cancelled(opendal_operator_rename_with_cancel(this->op, "seed.txt", "dst.txt", token));

    // 8. copy
    expect_cancelled(
        opendal_operator_copy_with_cancel(this->op, "seed.txt", "dst.txt", nullptr, token));

    // 9. check
    expect_cancelled(opendal_operator_check_with_cancel(this->op, token));

    // 10. list
    opendal_lister* lister_out = reinterpret_cast<opendal_lister*>(0x1);
    expect_cancelled(
        opendal_operator_list_with_cancel(this->op, "/", nullptr, token, &lister_out));
    EXPECT_EQ(lister_out, nullptr);

    // 11. reader
    opendal_reader* reader_out = reinterpret_cast<opendal_reader*>(0x1);
    expect_cancelled(
        opendal_operator_reader_with_cancel(this->op, "seed.txt", nullptr, token, &reader_out));
    EXPECT_EQ(reader_out, nullptr);

    // 12. writer
    opendal_writer* writer_out = reinterpret_cast<opendal_writer*>(0x1);
    expect_cancelled(
        opendal_operator_writer_with_cancel(this->op, "w.txt", nullptr, token, &writer_out));
    EXPECT_EQ(writer_out, nullptr);

    // 13. copier
    opendal_copier* copier_out = reinterpret_cast<opendal_copier*>(0x1);
    expect_cancelled(opendal_operator_copier_with_cancel(
        this->op, "seed.txt", "dst.txt", nullptr, token, &copier_out));
    EXPECT_EQ(copier_out, nullptr);

    // 14. deleter
    opendal_deleter* deleter_out = reinterpret_cast<opendal_deleter*>(0x1);
    expect_cancelled(opendal_operator_deleter_with_cancel(this->op, token, &deleter_out));
    EXPECT_EQ(deleter_out, nullptr);

    // 15-18. presign_read / write / delete / stat
    opendal_presigned_request* req_out = reinterpret_cast<opendal_presigned_request*>(0x1);
    expect_cancelled(opendal_operator_presign_read_with_cancel(
        this->op, "seed.txt", 60, nullptr, token, &req_out));
    EXPECT_EQ(req_out, nullptr);

    req_out = reinterpret_cast<opendal_presigned_request*>(0x1);
    expect_cancelled(opendal_operator_presign_write_with_cancel(
        this->op, "seed.txt", 60, nullptr, token, &req_out));
    EXPECT_EQ(req_out, nullptr);

    req_out = reinterpret_cast<opendal_presigned_request*>(0x1);
    expect_cancelled(opendal_operator_presign_delete_with_cancel(
        this->op, "seed.txt", 60, nullptr, token, &req_out));
    EXPECT_EQ(req_out, nullptr);

    req_out = reinterpret_cast<opendal_presigned_request*>(0x1);
    expect_cancelled(opendal_operator_presign_stat_with_cancel(
        this->op, "seed.txt", 60, nullptr, token, &req_out));
    EXPECT_EQ(req_out, nullptr);

    // Handle-level APIs with pre-cancelled token.
    opendal_reader* reader = nullptr;
    ASSERT_EQ(opendal_operator_reader(this->op, "seed.txt", &reader), nullptr);
    uint8_t buf[16] = { 0 };
    uintptr_t read_size = 77;
    expect_cancelled(opendal_reader_read_with_cancel(reader, buf, sizeof(buf), token, &read_size));
    EXPECT_EQ(read_size, 0u);
    uint64_t pos = 88;
    expect_cancelled(opendal_reader_seek_with_cancel(reader, 0, OPENDAL_SEEK_SET, token, &pos));
    EXPECT_EQ(pos, 0u);
    opendal_reader_free(reader);

    opendal_writer* writer = nullptr;
    ASSERT_EQ(opendal_operator_writer(this->op, "w2.txt", &writer), nullptr);
    uintptr_t write_size = 55;
    expect_cancelled(opendal_writer_write_with_cancel(writer, &seed, token, &write_size));
    EXPECT_EQ(write_size, 0u);
    opendal_metadata* wclose_meta = reinterpret_cast<opendal_metadata*>(0x1);
    expect_cancelled(opendal_writer_close_with_cancel(writer, token, &wclose_meta));
    EXPECT_EQ(wclose_meta, nullptr);
    opendal_writer_free(writer);

    opendal_lister* lister = nullptr;
    ASSERT_EQ(opendal_operator_list(this->op, "/", &lister), nullptr);
    opendal_entry* entry = reinterpret_cast<opendal_entry*>(0x1);
    expect_cancelled(opendal_lister_next_with_cancel(lister, token, &entry));
    EXPECT_EQ(entry, nullptr);
    opendal_lister_free(lister);

    opendal_deleter* deleter = nullptr;
    ASSERT_EQ(opendal_operator_deleter(this->op, &deleter), nullptr);
    expect_cancelled(opendal_deleter_delete_with_cancel(deleter, "seed.txt", nullptr, token));
    const char* del_paths[] = { "seed.txt" };
    expect_cancelled(opendal_deleter_delete_many_with_cancel(deleter, del_paths, 1, token));
    expect_cancelled(opendal_deleter_delete_many_with_cancel(deleter, nullptr, 0, token));
    expect_cancelled(opendal_deleter_flush_with_cancel(deleter, token));
    opendal_deleter_free(deleter);

    // Verify seed.txt was not deleted by the cancelled delete calls.
    bool still_exists = false;
    ASSERT_EQ(opendal_operator_exists(this->op, "seed.txt", &still_exists), nullptr);
    EXPECT_TRUE(still_exists);

    opendal_cancel_token_free(token);
}

TEST_F(OpendalCancelTest, ConcurrentCancelAndImmediateBufferFreeUnderAsan)
{
    std::vector<uint8_t> payload(64 * 1024, 0xAB);
    opendal_bytes data = {
        .data = payload.data(),
        .len = payload.size(),
        .capacity = payload.size(),
    };
    ASSERT_EQ(opendal_operator_write(this->op, "large.bin", &data), nullptr);

    // Run multiple rounds where background threads cancel the token concurrently
    // with reader reads, and the caller frees the read buffer immediately upon return.
    for (int round = 0; round < 32; ++round) {
        opendal_cancel_token* token = opendal_cancel_token_new();
        ASSERT_NE(token, nullptr);

        opendal_reader* reader = nullptr;
        ASSERT_EQ(opendal_operator_reader(this->op, "large.bin", &reader), nullptr);

        std::atomic<bool> start { false };
        std::thread canceler1([&]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            opendal_cancel_token_cancel(token);
        });
        std::thread canceler2([&]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            opendal_cancel_token_cancel(token);
        });

        start.store(true, std::memory_order_release);

        bool saw_cancelled = false;
        for (int iter = 0; iter < 64; ++iter) {
            uint8_t* heap_buf = static_cast<uint8_t*>(std::malloc(4096));
            ASSERT_NE(heap_buf, nullptr);
            uintptr_t nread = 1234;
            opendal_error* err = opendal_reader_read_with_cancel(reader, heap_buf, 4096, token, &nread);
            // Free buffer immediately upon return to detect any use-after-free under ASan.
            std::free(heap_buf);

            if (err != nullptr) {
                EXPECT_EQ(err->code, OPENDAL_CANCELLED);
                EXPECT_EQ(nread, 0u);
                opendal_error_free(err);
                saw_cancelled = true;
                break;
            }
            if (nread == 0) {
                uint64_t pos = 0;
                opendal_error* serr = opendal_reader_seek_with_cancel(reader, 0, OPENDAL_SEEK_SET, token, &pos);
                if (serr != nullptr) {
                    EXPECT_EQ(serr->code, OPENDAL_CANCELLED);
                    EXPECT_EQ(pos, 0u);
                    opendal_error_free(serr);
                    saw_cancelled = true;
                    break;
                }
            }
        }

        canceler1.join();
        canceler2.join();

        EXPECT_TRUE(opendal_cancel_token_is_cancelled(token));
        if (!saw_cancelled) {
            // Token is guaranteed cancelled now; a subsequent call must report OPENDAL_CANCELLED.
            uint8_t* heap_buf = static_cast<uint8_t*>(std::malloc(64));
            uintptr_t nread = 99;
            opendal_error* err = opendal_reader_read_with_cancel(reader, heap_buf, 64, token, &nread);
            std::free(heap_buf);
            ASSERT_NE(err, nullptr);
            EXPECT_EQ(err->code, OPENDAL_CANCELLED);
            EXPECT_EQ(nread, 0u);
            opendal_error_free(err);
        }

        opendal_reader_free(reader);
        opendal_cancel_token_free(token);
    }
}
