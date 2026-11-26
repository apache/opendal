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

import COpenDAL
import XCTest

@testable import OpenDAL

final class OpenDALTests: XCTestCase {
    func testSimple() throws {
        let op = try Operator(
            scheme: "memory",
            options: [
                "root": "/myroot"
            ]
        )

        var testContents = Data([1, 2, 3, 4])
        try op.blockingWrite(&testContents, to: "test")

        // The returned Data owns a Rust buffer freed by its deallocator
        // once the Data goes out of scope; dropping it must not crash.
        let readContents = try op.blockingRead("test")
        XCTAssertEqual(readContents, testContents)

        XCTAssertThrowsError(try op.blockingRead("test_not_exists"))
    }

    func testBlockingWriteReadEmptyData() throws {
        let op = try Operator(scheme: "memory")

        var empty = Data()
        try op.blockingWrite(&empty, to: "empty")

        let readContents = try op.blockingRead("empty")
        XCTAssertEqual(readContents, Data())
    }

    func testBlockingReadNotFoundError() throws {
        let op = try Operator(scheme: "memory")

        XCTAssertThrowsError(try op.blockingRead("not_exists")) { error in
            guard let opendalError = error as? OperatorError else {
                XCTFail("expected OperatorError, got \(error)")
                return
            }

            XCTAssertEqual(opendalError.code, OPENDAL_NOT_FOUND.rawValue)
            let message = String(data: opendalError.message, encoding: .utf8)
            XCTAssertNotNil(message)
            XCTAssertTrue(message!.contains("NotFound"), "unexpected error message: \(message!)")
        }
    }

    func testBlockingExistsAndDelete() throws {
        let op = try Operator(scheme: "memory")
        let path = "文档/café-🦀"

        XCTAssertFalse(try op.blockingExists(path))

        var data = Data([1, 2, 3, 4])
        try op.blockingWrite(&data, to: path)
        XCTAssertTrue(try op.blockingExists(path))

        try op.blockingDelete(path)
        XCTAssertFalse(try op.blockingExists(path))

        try op.blockingDelete(path)
        XCTAssertFalse(try op.blockingExists(path))
    }

    func testBlockingExistsAndDeleteRejectNulPath() throws {
        let op = try Operator(scheme: "memory")
        let path = "文档/café-🦀"
        let invalidPath = path + "\0suffix"
        var contents = Data([1, 2, 3, 4])
        try op.blockingWrite(&contents, to: path)

        func assertInvalidPath(_ error: Error) {
            guard let opendalError = error as? OperatorError else {
                XCTFail("expected OperatorError, got \(error)")
                return
            }

            XCTAssertEqual(opendalError.code, OPENDAL_UNEXPECTED.rawValue)
            let message = String(decoding: opendalError.message, as: UTF8.self)
            XCTAssertTrue(message.contains("U+0000"), "unexpected error message: \(message)")
        }

        XCTAssertThrowsError(try op.blockingExists(invalidPath), "exists", assertInvalidPath)
        XCTAssertThrowsError(try op.blockingDelete(invalidPath), "delete", assertInvalidPath)
        XCTAssertEqual(try op.blockingRead(path), contents)
    }

    func testBlockingDeleteNonEmptyDirectoryError() throws {
        let op = try Operator(scheme: "memory")
        let child = "directory/child"
        var data = Data([1])
        try op.blockingWrite(&data, to: child)

        XCTAssertThrowsError(try op.blockingDelete("directory/")) { error in
            guard let opendalError = error as? OperatorError else {
                XCTFail("expected OperatorError, got \(error)")
                return
            }

            XCTAssertEqual(opendalError.code, OPENDAL_UNEXPECTED.rawValue)
            let message = String(decoding: opendalError.message, as: UTF8.self)
            XCTAssertTrue(
                message.contains("directory not empty"),
                "unexpected error message: \(message)"
            )
        }
        XCTAssertTrue(try op.blockingExists(child))
    }

    func testOperatorInitInvalidSchemeError() throws {
        XCTAssertThrowsError(try Operator(scheme: "scheme_not_exist")) { error in
            guard let opendalError = error as? OperatorError else {
                XCTFail("expected OperatorError, got \(error)")
                return
            }

            let message = String(data: opendalError.message, encoding: .utf8)
            XCTAssertNotNil(message)
            XCTAssertFalse(message!.isEmpty)
        }
    }
}
