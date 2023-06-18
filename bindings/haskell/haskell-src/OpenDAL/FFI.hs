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
{-# LANGUAGE ForeignFunctionInterface #-}

module OpenDAL.FFI where

import Foreign
import Foreign.C.String
import Foreign.C.Types

data RawOperator

data FFIResult = FFIResult
  { ffiCode :: CUInt
  , dataPtr :: Ptr ()
  , errorMessage :: CString
  }
  deriving (Show)

instance Storable FFIResult where
  sizeOf _ = sizeOf (undefined :: CSize) + sizeOf (undefined :: Ptr ()) + sizeOf (undefined :: CString)
  alignment _ = alignment (undefined :: CSize)
  peek ptr = do
    s <- peekByteOff ptr codeOffset
    d <- peekByteOff ptr dataPtrOffset
    errMsg <- peekByteOff ptr errorMessageOffset
    return $ FFIResult s d errMsg
   where
    codeOffset = 0
    dataPtrOffset = sizeOf (undefined :: CSize)
    errorMessageOffset = dataPtrOffset + sizeOf (undefined :: Ptr ())
  poke ptr (FFIResult s d errMsg) = do
    pokeByteOff ptr codeOffset s
    pokeByteOff ptr dataPtrOffset d
    pokeByteOff ptr errorMessageOffset errMsg
   where
    codeOffset = 0
    dataPtrOffset = sizeOf (undefined :: CSize)
    errorMessageOffset = dataPtrOffset + sizeOf (undefined :: Ptr ())

data ByteSlice = ByteSlice
  { bsData :: Ptr CChar
  , bsLen :: CSize
  }

instance Storable ByteSlice where
  sizeOf _ = sizeOf (undefined :: Ptr CChar) + sizeOf (undefined :: CSize)
  alignment _ = alignment (undefined :: CSize)
  peek ptr = do
    bsDataPtr <- peekByteOff ptr dataOffset
    len <- peekByteOff ptr lenOffset
    return $ ByteSlice bsDataPtr len
   where
    dataOffset = 0
    lenOffset = sizeOf (undefined :: Ptr ())
  poke ptr (ByteSlice bsDataPtr len) = do
    pokeByteOff ptr dataOffset bsDataPtr
    pokeByteOff ptr lenOffset len
   where
    dataOffset = 0
    lenOffset = sizeOf (undefined :: Ptr ())

foreign import ccall "via_map_ffi"
  c_via_map_ffi ::
    CString -> Ptr CString -> Ptr CString -> CSize -> Ptr FFIResult -> IO ()
foreign import ccall "&free_operator" c_free_operator :: FunPtr (Ptr RawOperator -> IO ())
foreign import ccall "free_byteslice" c_free_byteslice :: Ptr CChar -> CSize -> IO ()
foreign import ccall "blocking_read" c_blocking_read :: Ptr RawOperator -> CString -> Ptr FFIResult -> IO ()
foreign import ccall "blocking_write" c_blocking_write :: Ptr RawOperator -> CString -> Ptr CChar -> CSize -> Ptr FFIResult -> IO ()
foreign import ccall "blocking_is_exist" c_blocking_is_exist :: Ptr RawOperator -> CString -> Ptr FFIResult -> IO ()
foreign import ccall "blocking_create_dir" c_blocking_create_dir :: Ptr RawOperator -> CString -> Ptr FFIResult -> IO ()
foreign import ccall "blocking_copy" c_blocking_copy :: Ptr RawOperator -> CString -> CString -> Ptr FFIResult -> IO ()
foreign import ccall "blocking_rename" c_blocking_rename :: Ptr RawOperator -> CString -> CString -> Ptr FFIResult -> IO ()
foreign import ccall "blocking_delete" c_blocking_delete :: Ptr RawOperator -> CString -> Ptr FFIResult -> IO ()