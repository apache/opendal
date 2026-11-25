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

module Main (main) where

import Control.Concurrent (forkIO, killThread)
import Control.Exception (bracket)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HashMap
import Network.Socket
import Network.Socket.ByteString (recv, sendAll)
import OpenDAL
import Test.Tasty (defaultMain)
import Test.Tasty.HUnit (Assertion, assertEqual, assertFailure, testCase)

main :: IO ()
main = defaultMain (testCase "stat preserves 64-bit metadata" testStatMetadata)

testStatMetadata :: Assertion
testStatMetadata = withSocketsDo $
  bracket (socket AF_INET Stream defaultProtocol) close $ \listener -> do
    bind listener (SockAddrInet 0 (tupleToHostAddress (127, 0, 0, 1)))
    listen listener 1
    SockAddrInet port _ <- getSocketName listener
    bracket (forkIO (serveHead listener)) killThread $ \_ -> do
      let endpoint = "http://127.0.0.1:" ++ show port
      newOperator "http" {ocConfig = HashMap.singleton "endpoint" endpoint} >>= \case
        Left err -> assertFailure $ "Failed to create HTTP operator: " ++ show err
        Right op -> statOpRaw op "large-object" >>= \case
          Left err -> assertFailure $ "HTTP stat failed: " ++ show err
          Right metadata -> do
            -- Above 4 GiB with nonzero low bits. C unsigned long is only 32 bits on Win64.
            assertEqual "Content-Length" 4294967313 (mContentLength metadata)
            assertEqual "ETag" (Just "\"large-object-etag\"") (mETag metadata)

serveHead :: Socket -> Assertion
serveHead listener = bracket (fst <$> accept listener) close $ \client -> do
  request <- readHeaders client BS.empty
  if "HEAD /large-object HTTP/1.1\r\n" `BS.isPrefixOf` request
    then sendAll client $
      BS.concat
        [ "HTTP/1.1 200 OK\r\n",
          "Content-Length: 4294967313\r\n",
          "ETag: \"large-object-etag\"\r\n",
          "Connection: close\r\n\r\n"
        ]
    else sendAll client "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
  where
    readHeaders client received
      | "\r\n\r\n" `BS.isInfixOf` received = return received
      | BS.length received >= 4096 = fail "HTTP request headers exceed 4 KiB"
      | otherwise = do
          chunk <- recv client (4096 - BS.length received)
          if BS.null chunk
            then fail "HTTP client closed before sending headers"
            else readHeaders client (received <> chunk)
