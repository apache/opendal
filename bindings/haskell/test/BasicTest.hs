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

module BasicTest (basicTests) where

import Colog (LogAction (LogAction), Msg (msgText))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.IORef
import qualified Data.Text as T
import OpenDAL
import Test.Tasty
import Test.Tasty.HUnit

basicTests :: TestTree
basicTests =
  testGroup
    "Basic Tests"
    [ testCase "testBasicOperation" testRawOperation,
      testCase "testMonad" testMonad,
      testCase "testError" testError
    ]

testRawOperation :: Assertion
testRawOperation = do
  Right op <- newOperator "memory"
  writeOpRaw op "key1" "value1" ?= Right ()
  writeOpRaw op "key2" "value2" ?= Right ()
  readOpRaw op "key1" ?= Right "value1"
  readOpRaw op "key2" ?= Right "value2"
  isExistOpRaw op "key1" ?= Right True
  isExistOpRaw op "key2" ?= Right True
  createDirOpRaw op "dir1/" ?= Right ()
  isExistOpRaw op "dir1/" ?= Right True
  statOpRaw op "key1" >>= \case
    Right meta -> meta @?= except_meta
    Left _ -> assertFailure "should not reach here"
  deleteOpRaw op "key1" ?= Right ()
  isExistOpRaw op "key1" ?= Right False
  where
    except_meta =
      Metadata
        { mMode = File,
          mCacheControl = Nothing,
          mContentDisposition = Nothing,
          mContentLength = 6,
          mContentMD5 = Nothing,
          mContentType = Nothing,
          mETag = Nothing,
          mLastModified = Nothing
        }

testMonad :: Assertion
testMonad = do
  Right op <- newOperator "memory"
  runOp op operation ?= Right ()
  where
    operation = do
      writeOp "key1" "value1"
      writeOp "key2" "value2"
      readOp "key1" ?= "value1"
      readOp "key2" ?= "value2"
      isExistOp "key1" ?= True
      isExistOp "key2" ?= True
      createDirOp "dir1/"
      isExistOp "dir1/" ?= True
      statOp "key1" ?= except_meta
      deleteOp "key1"
      isExistOp "key1" ?= False
    except_meta =
      Metadata
        { mMode = File,
          mCacheControl = Nothing,
          mContentDisposition = Nothing,
          mContentLength = 6,
          mContentMD5 = Nothing,
          mContentType = Nothing,
          mETag = Nothing,
          mLastModified = Nothing
        }

testError :: Assertion
testError = do
  Right op <- newOperator "memory"
  runOp op operation >>= \case
    Left err -> errorCode err @?= NotFound
    Right _ -> assertFailure "should not reach here"
  where
    operation = readOp "non-exist-path"

-- helper function

(?=) :: (MonadIO m, Eq a, Show a) => m a -> a -> m ()
result ?= except = result >>= liftIO . (@?= except)

findLister :: Lister -> String -> IO Bool
findLister lister key = do
  res <- nextLister lister
  case res of
    Left _ -> return False
    Right Nothing -> return False
    Right (Just k) -> if k == key then return True else findLister lister key
