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

    func testBlockingStatReturnsFileAndDirectoryMetadata() throws {
        let op = try Operator(scheme: "memory")
        let path = "文档/café-🦀"
        var data = Data([1, 2, 3, 4])
        try op.blockingWrite(&data, to: path)

        let file = try op.blockingStat(path)
        XCTAssertTrue(file.isFile)
        XCTAssertFalse(file.isDirectory)
        XCTAssertEqual(file.contentLength, 4)

        let root = try op.blockingStat("/")
        XCTAssertTrue(root.isDirectory)
        XCTAssertFalse(root.isFile)
        XCTAssertEqual(root.contentLength, 0)
        XCTAssertNil(root.lastModified)
    }

    func testBlockingStatErrors() throws {
        let op = try Operator(scheme: "memory")
        let path = "file"
        var data = Data([1])
        try op.blockingWrite(&data, to: path)

        XCTAssertThrowsError(try op.blockingStat("missing")) { error in
            XCTAssertEqual((error as? OperatorError)?.code, OPENDAL_NOT_FOUND.rawValue)
        }
        XCTAssertThrowsError(try op.blockingStat(path + "\0suffix")) { error in
            XCTAssertEqual((error as? OperatorError)?.code, OPENDAL_UNEXPECTED.rawValue)
        }
    }

    func testBlockingListerReturnsStandaloneEntriesWithoutRecursing() throws {
        let filePath = "文档/café-🦀"
        let entries: [Entry]
        do {
            let op = try Operator(scheme: "memory")
            var data = Data([1, 2, 3])
            try op.blockingCreateDirectory("文档/")
            try op.blockingWrite(&data, to: filePath)
            try op.blockingWrite(&data, to: "文档/sub/deep")

            let lister = try op.blockingLister("文档/")
            var listed: [Entry] = []
            while let entry = try lister.next() {
                listed.append(entry)
            }
            XCTAssertNil(try lister.next())
            entries = listed
        }

        XCTAssertEqual(entries.map(\.path).sorted(), ["文档/", filePath, "文档/sub/"])
        let file = try XCTUnwrap(entries.first { $0.path == filePath })
        XCTAssertEqual(file.name, "café-🦀")
        XCTAssertTrue(file.metadata.isFile)
        XCTAssertEqual(file.metadata.contentLength, 3)
        XCTAssertNil(file.metadata.lastModified)
        let directory = try XCTUnwrap(entries.first { $0.path == "文档/sub/" })
        XCTAssertEqual(directory.name, "sub/")
        XCTAssertTrue(directory.metadata.isDirectory)
    }

    func testBlockingListerRejectsNulPath() throws {
        let op = try Operator(scheme: "memory")
        try op.blockingCreateDirectory("文档/")

        XCTAssertThrowsError(try op.blockingLister("文档/\0suffix")) { error in
            XCTAssertEqual((error as? OperatorError)?.code, OPENDAL_UNEXPECTED.rawValue)
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

    func testBlockingCreateDirectory() throws {
        let op = try Operator(scheme: "memory")
        let path = "logs/archive/"

        XCTAssertFalse(try op.blockingExists(path))
        try op.blockingCreateDirectory(path)
        XCTAssertTrue(try op.blockingExists(path))

        try op.blockingCreateDirectory(path)
        XCTAssertTrue(try op.blockingExists(path))
    }

    func testBlockingCreateDirectoryRequiresTrailingSlash() throws {
        let op = try Operator(scheme: "memory")

        XCTAssertThrowsError(try op.blockingCreateDirectory("logs")) { error in
            XCTAssertEqual((error as? OperatorError)?.code, OPENDAL_NOT_A_DIRECTORY.rawValue)
        }
        XCTAssertFalse(try op.blockingExists("logs/"))
    }

    func testBlockingCreateDirectoryRejectsNulPath() throws {
        let op = try Operator(scheme: "memory")
        let prefix = "logs/"

        XCTAssertThrowsError(try op.blockingCreateDirectory(prefix + "\0suffix/")) { error in
            guard let opendalError = error as? OperatorError else {
                XCTFail("expected OperatorError, got \(error)")
                return
            }

            XCTAssertEqual(opendalError.code, OPENDAL_UNEXPECTED.rawValue)
            let message = String(decoding: opendalError.message, as: UTF8.self)
            XCTAssertTrue(message.contains("U+0000"), "unexpected error message: \(message)")
        }
        XCTAssertFalse(try op.blockingExists(prefix))
    }

    func testBlockingCopyRejectsSamePath() throws {
        let op = try Operator(scheme: "memory")

        XCTAssertThrowsError(try op.blockingCopy("source", to: "/./source")) { error in
            XCTAssertEqual((error as? OperatorError)?.code, OPENDAL_IS_SAME_FILE.rawValue)
        }
    }

    func testBlockingCopyRejectsNulPaths() throws {
        let op = try Operator(scheme: "memory")

        func assertInvalidPath(_ error: Error) {
            guard let opendalError = error as? OperatorError else {
                XCTFail("expected OperatorError, got \(error)")
                return
            }

            XCTAssertEqual(opendalError.code, OPENDAL_UNEXPECTED.rawValue)
            let message = String(decoding: opendalError.message, as: UTF8.self)
            XCTAssertTrue(message.contains("U+0000"), "unexpected error message: \(message)")
        }

        XCTAssertThrowsError(
            try op.blockingCopy("source\0suffix", to: "destination"),
            "source",
            assertInvalidPath
        )
        XCTAssertThrowsError(
            try op.blockingCopy("source", to: "destination\0suffix"),
            "destination",
            assertInvalidPath
        )
    }

    func testBlockingRenameRejectsSamePath() throws {
        let op = try Operator(scheme: "memory")

        XCTAssertThrowsError(try op.blockingRename("source", to: "/./source")) { error in
            XCTAssertEqual((error as? OperatorError)?.code, OPENDAL_IS_SAME_FILE.rawValue)
        }
    }

    func testBlockingRenameRejectsNulPaths() throws {
        let op = try Operator(scheme: "memory")

        func assertInvalidPath(_ error: Error) {
            guard let opendalError = error as? OperatorError else {
                XCTFail("expected OperatorError, got \(error)")
                return
            }

            XCTAssertEqual(opendalError.code, OPENDAL_UNEXPECTED.rawValue)
            let message = String(decoding: opendalError.message, as: UTF8.self)
            XCTAssertTrue(message.contains("U+0000"), "unexpected error message: \(message)")
        }

        XCTAssertThrowsError(
            try op.blockingRename("source\0suffix", to: "destination"),
            "source",
            assertInvalidPath
        )
        XCTAssertThrowsError(
            try op.blockingRename("source", to: "destination\0suffix"),
            "destination",
            assertInvalidPath
        )
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
