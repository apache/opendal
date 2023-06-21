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

import Control.Monad.IO.Class (liftIO)
import qualified Data.HashMap.Strict as HashMap
import OpenDAL
import Test.Tasty
import Test.Tasty.HUnit

basicTests :: TestTree
basicTests =
  testGroup
    "Basic Tests"
    [ testCase "testBasicOperation" testRawOperation
    , testCase "testMonad" testMonad
    , testCase "testError" testError
    ]

testRawOperation :: Assertion
testRawOperation = do
  Right op <- newOp "memory" HashMap.empty
  writeOpRaw op "key1" "value1" >>= (@?= Right ())
  writeOpRaw op "key2" "value2" >>= (@?= Right ())
  readOpRaw op "key1" >>= (@?= Right "value1")
  readOpRaw op "key2" >>= (@?= Right "value2")
  isExistOpRaw op "key1" >>= (@?= Right True)
  isExistOpRaw op "key2" >>= (@?= Right True)
  createDirOpRaw op "dir1/" >>= (@?= Right ())
  isExistOpRaw op "dir1/" >>= (@?= Right True)
  statOpRaw op "key1" >>= \v -> case v of
    Right meta -> meta @?= except_meta
    Left _ -> assertFailure "should not reach here"
  deleteOpRaw op "key1" >>= (@?= Right ())
  isExistOpRaw op "key1" >>= (@?= Right False)
 where
  except_meta =
    Metadata
      { mMode = File
      , mCacheControl = Nothing
      , mContentDisposition = Nothing
      , mContentLength = 6
      , mContentMD5 = Nothing
      , mContentRange = Nothing
      , mContentType = Nothing
      , mETag = Nothing
      , mLastModified = Nothing
      }

testMonad :: Assertion
testMonad = do
  Right op <- newOp "memory" HashMap.empty
  runOp op operation >>= (@?= Right ())
 where
  operation = do
    writeOp "key1" "value1"
    writeOp "key2" "value2"
    readOp "key1" >>= liftIO . (@?= "value1")
    readOp "key2" >>= liftIO . (@?= "value2")
    isExistOp "key1" >>= liftIO . (@?= True)
    isExistOp "key2" >>= liftIO . (@?= True)
    createDirOp "dir1/"
    isExistOp "dir1/" >>= liftIO . (@?= True)
    statOp "key1" >>= liftIO . (@?= except_meta)
    deleteOp "key1"
    isExistOp "key1" >>= liftIO . (@?= False)
  except_meta =
    Metadata
      { mMode = File
      , mCacheControl = Nothing
      , mContentDisposition = Nothing
      , mContentLength = 6
      , mContentMD5 = Nothing
      , mContentRange = Nothing
      , mContentType = Nothing
      , mETag = Nothing
      , mLastModified = Nothing
      }

testError :: Assertion
testError = do
  Right op <- newOp "memory" HashMap.empty
  runOp op operation >>= \v -> case v of
    Left err -> errorCode err @?= NotFound
    Right _ -> assertFailure "should not reach here"
 where
  operation = readOp "non-exist-path"
