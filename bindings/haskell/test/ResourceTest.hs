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

import Colog (LogAction (LogAction))
import Control.Exception (bracket)
import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HashMap
import Data.IORef (modifyIORef', newIORef, readIORef)
import GHC.IO.Encoding (getForeignEncoding, latin1, setForeignEncoding)
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
      testCase "testRepeatedWriterClose" testRepeatedWriterClose,
      testCase "testUtf8UnderLatin1Encoding" testUtf8UnderLatin1Encoding,
      testCase "testLoggerLifecycle" testLoggerLifecycle
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

testUtf8UnderLatin1Encoding :: Assertion
testUtf8UnderLatin1Encoding =
  bracket getForeignEncoding setForeignEncoding $ \_ -> do
    setForeignEncoding latin1
    Right op <-
      newOperator
        OperatorConfig
          { ocScheme = "memory",
            ocConfig = HashMap.fromList [("root", "/根目录-café/")],
            ocLogAction = Nothing
          }
    let dirPath = "目录-café/"
        filePath = "目录-café/日本語-file.txt"
        missingPath = "不存在/café.txt"
    createDirOpRaw op dirPath ?= Right ()
    writeOpRaw op filePath "hello-utf8" ?= Right ()
    isExistOpRaw op filePath ?= Right True
    readOpRaw op filePath ?= Right "hello-utf8"
    statOpRaw op filePath >>= \case
      Right meta -> mContentLength meta @?= 10
      Left err -> assertFailure $ "stat failed: " ++ show err
    Right lister <- listOpRaw op dirPath
    items <- collectListerItems lister
    assertBool ("expected UTF-8 path in listing, got: " ++ show items) (filePath `elem` items)
    readOpRaw op missingPath >>= \case
      Left err -> errorCode err @?= NotFound
      Right _ -> assertFailure "Expected NotFound for missing UTF-8 path"
    deleteOpRaw op filePath ?= Right ()
    isExistOpRaw op filePath ?= Right False
  where
    collectListerItems lister = go []
      where
        go acc = do
          r <- nextLister lister
          case r of
            Right (Just p) -> go (p : acc)
            Right Nothing -> return acc
            Left err -> assertFailure $ "lister error: " ++ show err

testLoggerLifecycle :: Assertion
testLoggerLifecycle = do
  let mkCounterLogger ref = LogAction (\_ -> modifyIORef' ref (+ (1 :: Int)))
      mkConfig scheme logger =
        OperatorConfig
          { ocScheme = scheme,
            ocConfig = HashMap.empty,
            ocLogAction = Just logger
          }

  -- Failed operator construction must not poison subsequent logger registration.
  failedCount <- newIORef (0 :: Int)
  newOperator (mkConfig "invalid-scheme-for-logger-test" (mkCounterLogger failedCount)) >>= \case
    Left _ -> return ()
    Right _ -> assertFailure "Expected invalid scheme to fail"

  -- First operator with logging succeeds and delivers log records.
  count1 <- newIORef (0 :: Int)
  Right op1 <- newOperator (mkConfig "memory" (mkCounterLogger count1))
  writeOpRaw op1 "log-key-1" "hello" ?= Right ()
  readOpRaw op1 "log-key-1" ?= Right "hello"
  n1 <- readIORef count1
  assertBool "expected first logger to receive log messages" (n1 > 0)

  -- Second operator with logging succeeds and switches to the new callback.
  count2 <- newIORef (0 :: Int)
  Right op2 <- newOperator (mkConfig "memory" (mkCounterLogger count2))
  writeOpRaw op2 "log-key-2" "world" ?= Right ()
  readOpRaw op2 "log-key-2" ?= Right "world"
  n2 <- readIORef count2
  assertBool "expected second logger to receive log messages" (n2 > 0)

  -- Subsequent failed operator construction must keep the active logger intact.
  newOperator (mkConfig "invalid-scheme-after-logger" (mkCounterLogger failedCount)) >>= \case
    Left _ -> return ()
    Right _ -> assertFailure "Expected invalid scheme to fail"
  beforeOp2 <- readIORef count2
  writeOpRaw op2 "log-key-3" "still-works" ?= Right ()
  afterOp2 <- readIORef count2
  assertBool "expected active logger to remain intact after failed newOperator" (afterOp2 > beforeOp2)
  nFailed <- readIORef failedCount
  nFailed @?= 0

  -- Exceptions thrown inside a LogAction must never unwind across the C boundary.
  Right op3 <- newOperator (mkConfig "memory" (LogAction (\_ -> fail "boom from log action")))
  writeOpRaw op3 "log-key-throw" "ok" ?= Right ()
  readOpRaw op3 "log-key-throw" ?= Right "ok"

  -- Restore a quiet no-op logger so later tests do not trigger the throwing logger.
  Right _ <- newOperator (mkConfig "memory" (LogAction (\_ -> return ())))
  performGC

-- helper function

(?=) :: (MonadIO m, Eq a, Show a) => m a -> a -> m ()
result ?= expected = result >>= liftIO . (@?= expected)
