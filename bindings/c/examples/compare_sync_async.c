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

#include "opendal.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

/* -------- Sync (blocking) -------- */
static void sync_example(void)
{
    opendal_result_operator_new r = opendal_operator_new("memory", NULL);
    const opendal_operator* op = r.op;

    const char* msg = "hello sync";
    opendal_bytes data = {
        .data = (uint8_t*)msg,
        .len = strlen(msg),
        .capacity = strlen(msg),
    };

    opendal_error* werr = opendal_operator_write(op, "sync.txt", &data);
    if (werr) {
        printf("sync write err: %d\n", werr->code);
        opendal_error_free(werr);
    }

    opendal_result_read rd = opendal_operator_read(op, "sync.txt");
    if (!rd.error) {
        printf("[sync] got %zu bytes: %.*s\n", rd.data.len, (int)rd.data.len, rd.data.data);
        opendal_bytes_free(&rd.data);
    } else {
        printf("sync read err: %d\n", rd.error->code);
        opendal_error_free(rd.error);
    }

    opendal_operator_delete(op, "sync.txt");
    opendal_operator_free(op);
}

/* -------- Async (future/await) -------- */
static void async_example(void)
{
    opendal_result_operator_new r = opendal_async_operator_new("memory", NULL);
    const opendal_async_operator* op = (const opendal_async_operator*)r.op;

    const char* msg = "hello async";
    opendal_bytes data = {
        .data = (uint8_t*)msg,
        .len = strlen(msg),
        .capacity = strlen(msg),
    };

    opendal_result_future_write wf = opendal_async_operator_write(op, "async.txt", &data);
    opendal_error* werr = opendal_future_write_await(wf.future);
    if (werr) {
        printf("async write err: %d\n", werr->code);
        opendal_error_free(werr);
    }

    // Kick off two reads without awaiting immediately to illustrate overlap.
    opendal_result_future_read rf1 = opendal_async_operator_read(op, "async.txt");
    opendal_result_future_read rf2 = opendal_async_operator_read(op, "async.txt");

    // ... do other work here (placeholder)
    printf("[async] doing other work before awaiting reads...\n");

    opendal_result_read rd1 = opendal_future_read_await(rf1.future);
    opendal_result_read rd2 = opendal_future_read_await(rf2.future);

    if (!rd1.error) {
        printf("[async] read1 %zu bytes: %.*s\n", rd1.data.len, (int)rd1.data.len, rd1.data.data);
        opendal_bytes_free(&rd1.data);
    } else {
        printf("async read1 err: %d\n", rd1.error->code);
        opendal_error_free(rd1.error);
    }

    if (!rd2.error) {
        printf("[async] read2 %zu bytes: %.*s\n", rd2.data.len, (int)rd2.data.len, rd2.data.data);
        opendal_bytes_free(&rd2.data);
    } else {
        printf("async read2 err: %d\n", rd2.error->code);
        opendal_error_free(rd2.error);
    }

    opendal_result_future_delete df = opendal_async_operator_delete(op, "async.txt");
    opendal_error* derr = opendal_future_delete_await(df.future);
    if (derr) {
        printf("async delete err: %d\n", derr->code);
        opendal_error_free(derr);
    }

    opendal_async_operator_free(op);
}

int main(void)
{
    printf("--- sync example ---\n");
    sync_example();

    printf("--- async example (blocking await) ---\n");
    async_example();

    // Non-blocking polling style: start a read, poll until ready, then await once.
    printf("--- async example (non-blocking poll) ---\n");
    opendal_result_operator_new r = opendal_async_operator_new("memory", NULL);
    const opendal_async_operator* op = (const opendal_async_operator*)r.op;

    const char* msg = "hello poll";
    opendal_bytes data = {.data = (uint8_t*)msg, .len = strlen(msg), .capacity = strlen(msg)};
    opendal_result_future_write wf = opendal_async_operator_write(op, "poll.txt", &data);
    opendal_error* werr = opendal_future_write_await(wf.future);
    if (werr) { opendal_error_free(werr); }

    opendal_result_future_read rf = opendal_async_operator_read(op, "poll.txt");
    opendal_result_read rd = {0};
    while (1) {
        opendal_future_status st = opendal_future_read_poll(rf.future, &rd);
        if (st == OPENDAL_FUTURE_PENDING) {
            // simulate doing other work
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 1 * 1000 * 1000; // 1ms
            nanosleep(&ts, NULL);
            continue;
        }
        break;
    }
    if (rd.error == NULL) {
        printf("[async poll] got %zu bytes: %.*s\n", rd.data.len, (int)rd.data.len, rd.data.data);
        opendal_bytes_free(&rd.data);
    } else {
        opendal_error_free(rd.error);
    }

    opendal_result_future_delete df = opendal_async_operator_delete(op, "poll.txt");
    opendal_error* derr = opendal_future_delete_await(df.future);
    if (derr) opendal_error_free(derr);

    opendal_async_operator_free(op);

    return 0;
}
