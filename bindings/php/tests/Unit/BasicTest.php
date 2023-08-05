<?php
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

it('opendal-php extension loaded', function () {
    expect(extension_loaded('opendal-php'))->toBeTrue();
});

it('class & methods exists', function ($class, $methods) {
    expect(class_exists($class))->toBeTrue();
    foreach ($methods as $method) {
        expect(method_exists($class, $method))->toBeTrue();
    }
})->with([
    ['OpenDAL\Operator', ['is_exist', 'read', 'write', 'delete', 'stat', 'create_dir']],
    ['OpenDAL\Metadata', []],
    ['OpenDAL\EntryMode', []],
]);

describe('throw exception', function () {
    it('invalid driver', function () {
        new \OpenDAL\Operator('invalid', []);
    })->throws('Exception');

    it('unspecified root path', function () {
        new \OpenDAL\Operator('fs', []);
    })->throws('Exception');

    it('read non-exist file', function () {
        $op = new \OpenDAL\Operator('fs', ['root' => '/tmp']);
        $op->read('non-exist.txt');
    })->throws('Exception');
});

it('initialization OpenDAL', function () {
    $op = new \OpenDAL\Operator('fs', ['root' => '/tmp']);

    expect($op)
        ->toBeInstanceOf(\OpenDAL\Operator::class)
        ->not->toHaveProperty('op')
        ->not->toThrow(Exception::class);
});
