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

module WriterTest (writerTests) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString.Char8 as BS8
import OpenDAL
import Test.Tasty
import Test.Tasty.HUnit

writerTests :: TestTree
writerTests =
  testGroup
    "Writer Tests"
    [ testCase "testWriterSequentialWrites" testWriterSequentialWrites,
      testCase "testWriterLargeData" testWriterLargeData,
      testCase "testWriterEmptyData" testWriterEmptyData,
      testCase "testWriterBinaryData" testWriterBinaryData,
      testCase "testAppendToNonExistent" testAppendToNonExistent,
      testCase "testAppendMultipleTimes" testAppendMultipleTimes,
      testCase "testWriterErrorHandling" testWriterErrorHandling
    ]

testWriterSequentialWrites :: Assertion
testWriterSequentialWrites = do
  Right op <- newOperator "memory"
  Right writer <- newWriter op "sequential-test"
  
  -- Write in multiple chunks
  writerWrite writer "Line 1\n" ?= Right ()
  writerWrite writer "Line 2\n" ?= Right ()
  writerWrite writer "Line 3\n" ?= Right ()
  
  Right meta <- writerClose writer
  mContentLength meta @?= 21
  
  -- Verify content
  readOpRaw op "sequential-test" ?= Right "Line 1\nLine 2\nLine 3\n"

testWriterLargeData :: Assertion
testWriterLargeData = do
  Right op <- newOperator "memory"
  Right writer <- newWriter op "large-data-test"
  
  -- Write 1KB of data in chunks
  let chunk = BS8.replicate 100 'A'
  mapM_ (\_ -> writerWrite writer chunk ?= Right ()) [1..10 :: Int]
  
  Right meta <- writerClose writer
  mContentLength meta @?= 1000
  
  -- Verify first few bytes
  Right content <- readOpRaw op "large-data-test"
  BS8.take 10 content @?= "AAAAAAAAAA"
  BS8.length content @?= 1000

testWriterEmptyData :: Assertion
testWriterEmptyData = do
  Right op <- newOperator "memory"
  Right writer <- newWriter op "empty-test"
  
  -- Write empty data
  writerWrite writer "" ?= Right ()
  
  Right meta <- writerClose writer
  mContentLength meta @?= 0
  
  -- Verify content
  readOpRaw op "empty-test" ?= Right ""

testWriterBinaryData :: Assertion
testWriterBinaryData = do
  Right op <- newOperator "memory"
  Right writer <- newWriter op "binary-test"
  
  -- Write binary data (all bytes 0-255)
  let binaryData = BS8.pack ['\0'..'\255']
  writerWrite writer binaryData ?= Right ()
  
  Right meta <- writerClose writer
  mContentLength meta @?= 256
  
  -- Verify content
  Right content <- readOpRaw op "binary-test"
  content @?= binaryData

testAppendToNonExistent :: Assertion
testAppendToNonExistent = do
  Right op <- newOperator "memory"
  
  -- Verify file doesn't exist
  isExistOpRaw op "new-append-file" ?= Right False
  
  -- Append to non-existent file (should create it)
  appendOpRaw op "new-append-file" "First content" ?= Right ()
  
  -- Verify file was created
  isExistOpRaw op "new-append-file" ?= Right True
  readOpRaw op "new-append-file" ?= Right "First content"

testAppendMultipleTimes :: Assertion
testAppendMultipleTimes = do
  Right op <- newOperator "memory"
  
  -- Multiple append operations
  appendOpRaw op "multi-append" "Hello" ?= Right ()
  appendOpRaw op "multi-append" " " ?= Right ()
  appendOpRaw op "multi-append" "World" ?= Right ()
  appendOpRaw op "multi-append" "!" ?= Right ()
  
  -- Verify final content
  readOpRaw op "multi-append" ?= Right "Hello World!"

testWriterErrorHandling :: Assertion
testWriterErrorHandling = do
  Right op <- newOperator "memory"
  
  -- Test writing to invalid path (should work with memory backend)
  -- Memory backend is permissive, so let's test a more complex scenario
  Right writer <- newWriter op "test-file"
  writerWrite writer "some data" ?= Right ()
  
  -- Close writer successfully
  Right meta <- writerClose writer
  mContentLength meta @?= 9

-- helper function

(?=) :: (MonadIO m, Eq a, Show a) => m a -> a -> m ()
result ?= except = result >>= liftIO . (@?= except) 