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

package org.apache.opendal.spring.core;

import java.util.concurrent.CompletableFuture;

/**
 * Interface that specified a basic set of Apache OpenDAL, implemented by {@link OpenDALTemplate}.
 */
public interface OpenDALOperations<T> {
    void write(String path, T entity);

    T read(String path);

    void delete(String path);

    CompletableFuture<Void> writeAsync(String path, T entity);

    CompletableFuture<T> readAsync(String path);

    CompletableFuture<Void> deleteAsync(String path);
}
