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

-- | Exercise the FFI resource cleanup paths: repeated operations, error
-- branches, lister exhaustion and ForeignPtr finalizers. A leaked or
-- mismatched free typically shows up as a crash or heap corruption under
-- repetition, especially after forcing the garbage collector.
module ResourceTest (resourceTests) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Data.ByteString as BS
import OpenDAL
import System.Mem (performGC)
import Test.Tasty
import Test.Tasty.HUnit

resourceTests :: TestTree
resourceTests =
  testGroup
    "Resource Cleanup Tests"
    [ testCase "testRepeatedReadOps" testRepeatedReadOps,
      testCase "testRepeatedErrorPaths" testRepeatedErrorPaths,
      testCase "testReadEmptyFile" testReadEmptyFile,
      testCase "testRepeatedStatOps" testRepeatedStatOps,
      testCase "testListerExhaustion" testListerExhaustion,
      testCase "testForeignPtrFinalizers" testForeignPtrFinalizers,
      testCase "testRepeatedWriterClose" testRepeatedWriterClose
    ]

testRepeatedReadOps :: Assertion
testRepeatedReadOps = do
  Right op <- newOperator "memory"
  writeOpRaw op "rr" "v" ?= Right ()
  -- Every read allocates a ByteSlice plus its wrapper; freeing must match on
  -- every iteration, not just the first.
  mapM_
    (\i -> readOpRaw op "rr" ?= Right "v")
    ([1 .. 200] :: [Int])
  performGC

testRepeatedErrorPaths :: Assertion
testRepeatedErrorPaths = do
  Right op <- newOperator "memory"
  -- Each iteration allocates an error_message CString that must be released
  -- through free_string, on raw and monadic paths alike.
  mapM_
    ( \i -> do
        Left err <- readOpRaw op ("missing-" ++ show i)
        errorCode err @?= NotFound
    )
    ([1 .. 100] :: [Int])
  runOp op (readOp "missing") >>= \case
    Left err -> errorCode err @?= NotFound
    Right _ -> assertFailure "Expected NotFound error"
  performGC

testReadEmptyFile :: Assertion
testReadEmptyFile = do
  Right op <- newOperator "memory"
  -- Zero-length ByteSlice: freeing an empty buffer must not crash.
  writeOpRaw op "empty" "" ?= Right ()
  readOpRaw op "empty" ?= Right BS.empty
  statOpRaw op "empty" >>= \case
    Right meta -> mContentLength meta @?= 0
    Left err -> assertFailure $ "stat failed: " ++ show err

testRepeatedStatOps :: Assertion
testRepeatedStatOps = do
  Right op <- newOperator "memory"
  writeOpRaw op "stat-me" "12345" ?= Right ()
  -- Each stat allocates a Metadata box plus CString fields, all released by
  -- free_metadata.
  mapM_
    ( \i ->
        statOpRaw op "stat-me" >>= \case
          Right meta -> mContentLength meta @?= 5
          Left err -> assertFailure $ "stat failed: " ++ show err
    )
    ([1 .. 100] :: [Int])
  performGC

testListerExhaustion :: Assertion
testListerExhaustion = do
  Right op <- newOperator "memory"
  writeOpRaw op "res-dir/a" "1" ?= Right ()
  writeOpRaw op "res-dir/b" "2" ?= Right ()
  Right lister <- listOpRaw op "res-dir/"
  items <- collectAll lister
  assertBool "expected a in listing" ("res-dir/a" `elem` items)
  assertBool "expected b in listing" ("res-dir/b" `elem` items)
  -- Next after exhaustion must keep returning Right Nothing without
  -- allocating or crashing.
  nextLister lister ?= Right Nothing
  where
    collectAll l = go []
      where
        go acc = do
          r <- nextLister l
          case r of
            Right (Just p) -> go (p : acc)
            Right Nothing -> return acc
            Left err -> assertFailure $ "lister error: " ++ show err

testForeignPtrFinalizers :: Assertion
testForeignPtrFinalizers = do
  -- Scope every handle so the GC can run its finalizer; a mismatched
  -- allocator or double-free would crash here.
  do
    Right op <- newOperator "memory"
    Right lister <- listOpRaw op "/"
    Right writer <- writerOpRaw op "gc-file" defaultWriterOption
    writerWrite writer "x" ?= Right ()
    _ <- nextLister lister
    return ()
  performGC
  performGC

testRepeatedWriterClose :: Assertion
testRepeatedWriterClose = do
  Right op <- newOperator "memory"
  -- Each writerClose returns a Metadata box released by free_metadata; the
  -- writer itself is released by the ForeignPtr finalizer.
  mapM_
    ( \i -> do
        Right writer <- writerOpRaw op ("wr-" ++ show i) defaultWriterOption
        writerWrite writer "data" ?= Right ()
        writerClose writer >>= \case
          Right meta -> mContentLength meta @?= 4
          Left err -> assertFailure $ "close failed: " ++ show err
    )
    ([1 .. 50] :: [Int])
  performGC

-- helper function

(?=) :: (MonadIO m, Eq a, Show a) => m a -> a -> m ()
result ?= expected = result >>= liftIO . (@?= expected)
