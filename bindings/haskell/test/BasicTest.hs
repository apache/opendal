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
  op <- createOp "memory" HashMap.empty
  case op of
    Left e -> assertFailure $ "Failed to create operator, " ++ e
    Right op' -> do
      _ <- writeOp op' "key1" "value1"
      _ <- writeOp op' "key2" "value2"
      value1 <- readOp op' "key1"
      value2 <- readOp op' "key2"
      value1 @?= Right "value1"
      value2 @?= Right "value2"