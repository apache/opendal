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

module PerformanceTest (performanceTests) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString.Char8 as BS8
import Data.Time
import OpenDAL
import Test.Tasty
import Test.Tasty.HUnit

performanceTests :: TestTree
performanceTests =
  testGroup
    "Performance Tests"
    [ testCase "testBulkOperations" testBulkOperations,
      testCase "testWriterVsDirectWrite" testWriterVsDirectWrite,
      testCase "testLargeFileOperations" testLargeFileOperations,
      testCase "testConcurrentOperations" testConcurrentOperations
    ]

testBulkOperations :: Assertion
testBulkOperations = do
  Right op <- newOperator "memory"
  
  -- Test bulk write operations
  start <- getCurrentTime
  mapM_ (\i -> writeOpRaw op ("bulk-file-" ++ show i) ("content-" ++ show i) ?= Right ()) [1..100]
  writeEnd <- getCurrentTime
  
  -- Test bulk read operations  
  mapM_ (\i -> readOpRaw op ("bulk-file-" ++ show i) ?= Right ("content-" ++ show i)) [1..100]
  readEnd <- getCurrentTime
  
  let writeTime = diffUTCTime writeEnd start
      readTime = diffUTCTime readEnd writeEnd
  
  -- Just verify operations completed (performance checks are informational)
  putStrLn $ "Bulk write time: " ++ show writeTime
  putStrLn $ "Bulk read time: " ++ show readTime
  
  -- Verify correctness
  readOpRaw op "bulk-file-50" ?= Right "content-50"

testWriterVsDirectWrite :: Assertion
testWriterVsDirectWrite = do
  Right op <- newOperator "memory"
  
  let testData = BS8.replicate 1000 'A'
  
  -- Test writer approach
  start1 <- getCurrentTime
  Right writer <- newWriter op "writer-test"
  mapM_ (\_ -> writerWrite writer "chunk" ?= Right ()) [1..100]
  Right _ <- writerClose writer
  end1 <- getCurrentTime
  
  -- Test direct write approach  
  start2 <- getCurrentTime
  let combinedData = BS8.concat $ replicate 100 "chunk"
  writeOpRaw op "direct-test" combinedData ?= Right ()
  end2 <- getCurrentTime
  
  let writerTime = diffUTCTime end1 start1
      directTime = diffUTCTime end2 start2
  
  putStrLn $ "Writer approach time: " ++ show writerTime
  putStrLn $ "Direct write time: " ++ show directTime
  
  -- Verify both approaches produce same result
  Right writerContent <- readOpRaw op "writer-test"
  Right directContent <- readOpRaw op "direct-test"
  writerContent @?= directContent

testLargeFileOperations :: Assertion
testLargeFileOperations = do
  Right op <- newOperator "memory"
  
  -- Create a 1MB file using writer
  let chunkSize = 1024 -- 1KB chunks
      numChunks = 1024 -- 1024 chunks = 1MB
      chunk = BS8.replicate chunkSize 'X'
  
  start <- getCurrentTime
  Right writer <- newWriter op "large-file"
  mapM_ (\_ -> writerWrite writer chunk ?= Right ()) [1..numChunks]
  Right meta <- writerClose writer
  end <- getCurrentTime
  
  let writeTime = diffUTCTime end start
  putStrLn $ "Large file (1MB) write time: " ++ show writeTime
  
  -- Verify file size
  mContentLength meta @?= fromIntegral (chunkSize * numChunks)
  
  -- Test reading the large file
  start2 <- getCurrentTime
  Right content <- readOpRaw op "large-file"
  end2 <- getCurrentTime
  
  let readTime = diffUTCTime end2 start2
  putStrLn $ "Large file (1MB) read time: " ++ show readTime
  
  -- Verify content correctness (just check size and first/last bytes)
  BS8.length content @?= chunkSize * numChunks
  BS8.head content @?= 'X'
  BS8.last content @?= 'X'

testConcurrentOperations :: Assertion
testConcurrentOperations = do
  Right op <- newOperator "memory"
  
  -- Simulate concurrent operations by interleaving writes and reads
  start <- getCurrentTime
  
  -- Write some files
  mapM_ (\i -> writeOpRaw op ("concurrent-" ++ show i) ("data-" ++ show i) ?= Right ()) [1..50]
  
  -- Read while writing more
  mapM_ (\i -> do
    writeOpRaw op ("concurrent-extra-" ++ show i) ("extra-" ++ show i) ?= Right ()
    readOpRaw op ("concurrent-" ++ show i) ?= Right ("data-" ++ show i)
    ) [1..25]
  
  end <- getCurrentTime
  let totalTime = diffUTCTime end start
  putStrLn $ "Concurrent operations time: " ++ show totalTime
  
  -- Verify some operations completed correctly
  readOpRaw op "concurrent-10" ?= Right "data-10"
  readOpRaw op "concurrent-extra-10" ?= Right "extra-10"

-- helper function

(?=) :: (MonadIO m, Eq a, Show a) => m a -> a -> m ()
result ?= except = result >>= liftIO . (@?= except) 