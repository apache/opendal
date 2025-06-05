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

-- | Comprehensive example demonstrating OpenDAL Haskell binding usage
module Main where

import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Char8 as BS8
import OpenDAL

main :: IO ()
main = do
  putStrLn "=== OpenDAL Haskell Binding Example ==="
  
  -- Create operator
  putStrLn "\n1. Creating memory operator..."
  Right op <- newOperator "memory"
  putStrLn "✓ Operator created successfully"
  
  -- Basic operations
  putStrLn "\n2. Basic file operations..."
  basicOperations op
  
  -- Writer operations
  putStrLn "\n3. Writer operations..."
  writerOperations op
  
  -- Append operations
  putStrLn "\n4. Append operations..."
  appendOperations op
  
  -- Directory operations
  putStrLn "\n5. Directory operations..."
  directoryOperations op
  
  -- Listing operations
  putStrLn "\n6. Listing operations..."
  listingOperations op
  
  -- Monad operations
  putStrLn "\n7. Monad operations..."
  monadOperations op
  
  -- Error handling
  putStrLn "\n8. Error handling..."
  errorHandling op
  
  putStrLn "\n=== Example completed successfully! ==="

basicOperations :: Operator -> IO ()
basicOperations op = do
  -- Write and read
  Right () <- writeOpRaw op "hello.txt" "Hello, World!"
  putStrLn "✓ File written"
  
  Right content <- readOpRaw op "hello.txt"
  putStrLn $ "✓ File read: " ++ BS8.unpack content
  
  -- Check existence
  Right exists <- isExistOpRaw op "hello.txt"
  putStrLn $ "✓ File exists: " ++ show exists
  
  -- Get metadata
  Right meta <- statOpRaw op "hello.txt"
  putStrLn $ "✓ File size: " ++ show (mContentLength meta) ++ " bytes"
  
  -- Copy file
  Right () <- copyOpRaw op "hello.txt" "hello-copy.txt"
  putStrLn "✓ File copied"
  
  -- Rename file
  Right () <- renameOpRaw op "hello-copy.txt" "hello-renamed.txt"
  putStrLn "✓ File renamed"
  
  -- Delete file
  Right () <- deleteOpRaw op "hello-renamed.txt"
  putStrLn "✓ File deleted"

writerOperations :: Operator -> IO ()
writerOperations op = do
  -- Create writer
  Right writer <- newWriter op "writer-demo.txt"
  putStrLn "✓ Writer created"
  
  -- Write in chunks
  Right () <- writerWrite writer "Line 1\n"
  Right () <- writerWrite writer "Line 2\n"
  Right () <- writerWrite writer "Line 3\n"
  putStrLn "✓ Data written in chunks"
  
  -- Close writer
  Right meta <- writerClose writer
  putStrLn $ "✓ Writer closed, final size: " ++ show (mContentLength meta)
  
  -- Verify content
  Right content <- readOpRaw op "writer-demo.txt"
  putStrLn $ "✓ Final content: " ++ show (BS8.unpack content)

appendOperations :: Operator -> IO ()
appendOperations op = do
  -- Initial write
  Right () <- writeOpRaw op "append-demo.txt" "Initial content"
  putStrLn "✓ Initial content written"
  
  -- Append using direct append
  Right () <- appendOpRaw op "append-demo.txt" " + appended"
  putStrLn "✓ Content appended directly"
  
  -- Append using writer
  Right writer <- newWriterAppend op "append-demo.txt"
  Right () <- writerWrite writer " + writer append"
  Right meta <- writerClose writer
  putStrLn "✓ Content appended via writer"
  
  -- Check final content
  Right content <- readOpRaw op "append-demo.txt"
  putStrLn $ "✓ Final content: " ++ BS8.unpack content

directoryOperations :: Operator -> IO ()
directoryOperations op = do
  -- Create directory
  Right () <- createDirOpRaw op "demo-dir/"
  putStrLn "✓ Directory created"
  
  -- Create files in directory
  Right () <- writeOpRaw op "demo-dir/file1.txt" "File 1 content"
  Right () <- writeOpRaw op "demo-dir/file2.txt" "File 2 content"
  putStrLn "✓ Files created in directory"
  
  -- Create subdirectory
  Right () <- createDirOpRaw op "demo-dir/subdir/"
  Right () <- writeOpRaw op "demo-dir/subdir/file3.txt" "File 3 content"
  putStrLn "✓ Subdirectory and file created"
  
  -- Remove all
  Right () <- removeAllOpRaw op "demo-dir/"
  putStrLn "✓ Directory and contents removed"

listingOperations :: Operator -> IO ()
listingOperations op = do
  -- Create test structure
  Right () <- writeOpRaw op "list-demo/a.txt" "A"
  Right () <- writeOpRaw op "list-demo/b.txt" "B"
  Right () <- createDirOpRaw op "list-demo/subdir/"
  Right () <- writeOpRaw op "list-demo/subdir/c.txt" "C"
  putStrLn "✓ Test structure created"
  
  -- List directory (non-recursive)
  Right lister <- listOpRaw op "list-demo/"
  putStrLn "✓ Directory listing (non-recursive):"
  printListerContents lister "  "
  
  -- Scan directory (recursive)
  Right scanner <- scanOpRaw op "list-demo/"
  putStrLn "✓ Directory scanning (recursive):"
  printListerContents scanner "  "

printListerContents :: Lister -> String -> IO ()
printListerContents lister prefix = do
  result <- nextLister lister
  case result of
    Right (Just item) -> do
      putStrLn $ prefix ++ item
      printListerContents lister prefix
    Right Nothing -> return ()
    Left err -> putStrLn $ prefix ++ "Error: " ++ show err

monadOperations :: Operator -> IO ()
monadOperations op = do
  result <- runOp op $ do
    -- Write multiple files
    writeOp "monad-demo/file1.txt" "Content 1"
    writeOp "monad-demo/file2.txt" "Content 2"
    writeOp "monad-demo/file3.txt" "Content 3"
    
    -- Read them back
    content1 <- readOp "monad-demo/file1.txt"
    content2 <- readOp "monad-demo/file2.txt"
    content3 <- readOp "monad-demo/file3.txt"
    
    -- Check existence
    exists1 <- isExistOp "monad-demo/file1.txt"
    exists2 <- isExistOp "monad-demo/file2.txt"
    
    -- Append to file
    appendOp "monad-demo/file1.txt" " + appended"
    
    -- Get final content
    finalContent <- readOp "monad-demo/file1.txt"
    
    liftIO $ do
      putStrLn $ "✓ File 1 content: " ++ BS8.unpack content1
      putStrLn $ "✓ File 2 content: " ++ BS8.unpack content2
      putStrLn $ "✓ File 3 content: " ++ BS8.unpack content3
      putStrLn $ "✓ File 1 exists: " ++ show exists1
      putStrLn $ "✓ File 2 exists: " ++ show exists2
      putStrLn $ "✓ Final content: " ++ BS8.unpack finalContent
    
    return "Monad operations completed"
  
  case result of
    Right message -> putStrLn $ "✓ " ++ message
    Left err -> putStrLn $ "✗ Error: " ++ show err

errorHandling :: Operator -> IO ()
errorHandling op = do
  -- Try to read non-existent file
  result <- readOpRaw op "nonexistent.txt"
  case result of
    Left err -> putStrLn $ "✓ Expected error caught: " ++ show (errorCode err)
    Right _ -> putStrLn "✗ Unexpected success"
  
  -- Try to stat non-existent file
  statResult <- statOpRaw op "nonexistent.txt"
  case statResult of
    Left err -> putStrLn $ "✓ Stat error caught: " ++ show (errorCode err)
    Right _ -> putStrLn "✗ Unexpected success"
  
  -- Error in monad should propagate
  monadResult <- runOp op $ do
    writeOp "test.txt" "content"
    readOp "nonexistent.txt" -- This should fail
    writeOp "should-not-reach.txt" "content"
    return "Should not reach here"
  
  case monadResult of
    Left err -> putStrLn $ "✓ Monad error propagated: " ++ show (errorCode err)
    Right _ -> putStrLn "✗ Error should have propagated" 