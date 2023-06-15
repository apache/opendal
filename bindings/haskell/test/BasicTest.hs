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
    [ testCase "read and write to memory" testReadAndWriteToMemory
    ]

testReadAndWriteToMemory :: Assertion
testReadAndWriteToMemory = do
  Right op <- createOp "memory" HashMap.empty
  _ <- writeOp op "key1" "value1"
  _ <- writeOp op "key2" "value2"
  value1 <- readOp op "key1"
  value2 <- readOp op "key2"
  value1 @?= Right "value1"
  value2 @?= Right "value2"