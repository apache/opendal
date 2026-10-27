// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static pthread_once_t initialized = PTHREAD_ONCE_INIT;
static int (*next_open)(const char *, int, ...);
static int (*next_open64)(const char *, int, ...);
static char *root;
static size_t root_len;
static _Atomic unsigned long reads, writes;

static void initialize(void) {
    next_open = dlsym(RTLD_NEXT, "open");
    next_open64 = dlsym(RTLD_NEXT, "open64");
    const char *value = getenv("OPENDAL_BENCH_DIRECT_ROOT");
    if (!next_open || !next_open64 || !value || !(root = realpath(value, NULL))) {
        fputs("direct I/O helper requires OPENDAL_BENCH_DIRECT_ROOT\n", stderr);
        _exit(1);
    }
    root_len = strlen(root);
}

/* Queried by the benchmark so a missing preload cannot silently use page cache. */
int opendal_bench_direct_for(const char *path) {
    pthread_once(&initialized, initialize);
    return strncmp(path, root, root_len) == 0 && path[root_len] == '/';
}

static int open_file(const char *path, int flags, mode_t mode, int large) {
    pthread_once(&initialized, initialize);
    int direct = opendal_bench_direct_for(path) && !(flags & (O_DIRECTORY | O_PATH));
    int fd = (large ? next_open64 : next_open)(path, flags | (direct ? O_DIRECT : 0), mode);
    if (fd >= 0 && direct) {
        int actual = fcntl(fd, F_GETFL);
        if (actual < 0 || !(actual & O_DIRECT)) {
            close(fd);
            errno = EIO;
            return -1;
        }
        if ((flags & O_ACCMODE) == O_RDONLY) {
            atomic_fetch_add(&reads, 1);
        } else {
            atomic_fetch_add(&writes, 1);
        }
    }
    return fd;
}

int open(const char *path, int flags, ...) {
    mode_t mode = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    return open_file(path, flags, mode, 0);
}

int open64(const char *path, int flags, ...) {
    mode_t mode = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    return open_file(path, flags, mode, 1);
}

__attribute__((destructor)) static void report(void) {
    fprintf(stderr, "direct_io: verified_read_opens=%lu verified_write_opens=%lu\n",
            atomic_load(&reads), atomic_load(&writes));
}
