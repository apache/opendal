-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
{-# LANGUAGE OverloadedStrings #-}

module BasicTest (basicTests) where

import qualified Data.HashMap.Strict as HashMap
import OpenDAL
import Test.Tasty
import Test.Tasty.HUnit

basicTests :: TestTree
basicTests =
  testGroup
    "Basic Tests"
    [ testCase "test-memory" testMemory
    , testCase "test-fs" testFs
    ]

testMemory :: Assertion
testMemory = do
  Right op <- newOp "memory" $ HashMap.empty
  writeOp op "key1" "value1" >>= (@?= Right ())
  writeOp op "key2" "value2" >>= (@?= Right ())
  readOp op "key1" >>= (@?= Right "value1")
  readOp op "key2" >>= (@?= Right "value2")
  isExistOp op "key1" >>= (@?= Right True)
  isExistOp op "key2" >>= (@?= Right True)
  createDirOp op "dir1/" >>= (@?= Right ())
  isExistOp op "dir1/" >>= (@?= Right True)
  deleteOp op "key1" >>= (@?= Right ())
  isExistOp op "key1" >>= (@?= Right False)

testFs :: Assertion
testFs = do
  Right op <- newOp "fs" $ HashMap.fromList [("root", "/tmp/opendal-test")]
  writeOp op "key1" "value1" >>= (@?= Right ())
  writeOp op "key2" "value2" >>= (@?= Right ())
  readOp op "key1" >>= (@?= Right "value1")
  readOp op "key2" >>= (@?= Right "value2")
  isExistOp op "key1" >>= (@?= Right True)
  isExistOp op "key2" >>= (@?= Right True)
  createDirOp op "dir1/" >>= (@?= Right ())
  isExistOp op "dir1/" >>= (@?= Right True)
  copyOp op "key1" "key3" >>= (@?= Right ())
  isExistOp op "key3" >>= (@?= Right True)
  isExistOp op "key1" >>= (@?= Right True)
  renameOp op "key2" "key4" >>= (@?= Right ())
  isExistOp op "key4" >>= (@?= Right True)
  isExistOp op "key2" >>= (@?= Right False)
  deleteOp op "key1" >>= (@?= Right ())
  isExistOp op "key1" >>= (@?= Right False)