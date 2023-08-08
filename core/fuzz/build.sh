# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# build fuzz targets
cd $SRC/opendal/core
cargo +nightly fuzz build -O --debug-assertions

# copy fuzz targets to $OUT

cp ../target/x86_64-unknown-linux-gnu/release/fuzz_reader $OUT/fuzz_reader_fs
cp ../target/x86_64-unknown-linux-gnu/release/fuzz_reader $OUT/fuzz_writer_fs

cp ../target/x86_64-unknown-linux-gnu/release/fuzz_reader $OUT/fuzz_reader_mem
cp ../target/x86_64-unknown-linux-gnu/release/fuzz_reader $OUT/fuzz_writer_mem

# .options files will be used by oss-fuzz to append arguments to the fuzz targets
echo -e "[libfuzzer]\nOPENDAL_FS_TEST=on\nOPENDAL_FS_ROOT=/tmp" > $SRC/opendal/fuzz_reader_fs.options
echo -e "[libfuzzer]\nOPENDAL_MEMORY_TEST=on\n" > $SRC/opendal/fuzz_reader_mem.options

echo -e "[libfuzzer]\nOPENDAL_FS_TEST=on\nOPENDAL_FS_ROOT=/tmp" > $SRC/opendal/fuzz_writer_fs.options
echo -e "[libfuzzer]\nOPENDAL_MEMORY_TEST=on\n" > $SRC/opendal/fuzz_writer_mem.options

# copy .options files to $OUT
cp $SRC/opendal/fuzz_reader*.options $OUT/
cp $SRC/opendal/fuzz_writer*.options $OUT/
