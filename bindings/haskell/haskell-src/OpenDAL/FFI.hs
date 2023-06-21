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
{-# LANGUAGE ScopedTypeVariables #-}

module OpenDAL.FFI where

import Foreign
import Foreign.C.String
import Foreign.C.Types

data RawOperator

data FFIResult a = FFIResult
  { ffiCode :: CUInt
  , dataPtr :: Ptr a
  , errorMessage :: CString
  }
  deriving (Show)

instance Storable (FFIResult a) where
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

data FFIMaybe a = FFIMaybe
  { ffiMaybeCode :: CBool
  , ffiMaybeData :: a
  }
  deriving (Show)

instance forall a. (Storable a) => Storable (FFIMaybe a) where
  sizeOf _ = sizeOf (undefined :: CSize) + (sizeOf (undefined :: a))
  alignment _ = alignment (undefined :: CSize)
  peek ptr = do
    s <- peekByteOff ptr codeOffset
    d <- peekByteOff ptr dataOffset
    return $ FFIMaybe s d
   where
    codeOffset = 0
    dataOffset = sizeOf (undefined :: CSize)
  poke ptr (FFIMaybe s d) = do
    pokeByteOff ptr codeOffset s
    pokeByteOff ptr dataOffset d
   where
    codeOffset = 0
    dataOffset = sizeOf (undefined :: CSize)

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

data FFIBytesContentRange = FFIBytesContentRange
  { ffibcrStart :: Word64
  , ffibcrEnd :: Word64
  , ffibcrSize :: Word64
  }
  deriving (Eq, Show)

instance Storable FFIBytesContentRange where
  sizeOf _ = sizeOf (undefined :: Word64) * 3
  alignment _ = alignment (undefined :: CSize)
  peek ptr = do
    start <- peekByteOff ptr startOffset
    end <- peekByteOff ptr endOffset
    size <- peekByteOff ptr sizeOffset
    return $ FFIBytesContentRange start end size
   where
    startOffset = 0
    endOffset = sizeOf (undefined :: Word64)
    sizeOffset = endOffset + sizeOf (undefined :: Word64)
  poke ptr (FFIBytesContentRange start end size) = do
    pokeByteOff ptr startOffset start
    pokeByteOff ptr endOffset end
    pokeByteOff ptr sizeOffset size
   where
    startOffset = 0
    endOffset = sizeOf (undefined :: Word64)
    sizeOffset = endOffset + sizeOf (undefined :: Word64)

data FFIMetadata = FFIMetadata
  { ffiMode :: CUInt
  , ffiCacheControl :: CString
  , ffiContentDisposition :: CString
  , ffiContentLength :: CULong
  , ffiContentMD5 :: CString
  , ffiContentRange :: FFIMaybe FFIBytesContentRange
  , ffiContentType :: CString
  , ffiETag :: CString
  , ffiLastModified :: FFIMaybe CTime
  }
  deriving (Show)

instance Storable FFIMetadata where
  sizeOf _ = sizeOf (undefined :: CSize) + sizeOf (undefined :: CString) * 5 + sizeOf (undefined :: CULong) + sizeOf (undefined :: FFIMaybe FFIBytesContentRange) + sizeOf (undefined :: FFIMaybe CTime)
  alignment _ = alignment (undefined :: CSize)
  peek ptr = do
    mode <- peekByteOff ptr modeOffset
    cacheControl <- peekByteOff ptr cacheControlOffset
    contentDisposition <- peekByteOff ptr contentDispositionOffset
    contentLength <- peekByteOff ptr contentLengthOffset
    contentMD5 <- peekByteOff ptr contentMD5Offset
    contentRange <- peekByteOff ptr contentRangeOffset
    contentType <- peekByteOff ptr contentTypeOffset
    eTag <- peekByteOff ptr eTagOffset
    lastModified <- peekByteOff ptr lastModifiedOffset
    return $ FFIMetadata mode cacheControl contentDisposition contentLength contentMD5 contentRange contentType eTag lastModified
   where
    modeOffset = 0
    cacheControlOffset = modeOffset + sizeOf (undefined :: CSize)
    contentDispositionOffset = cacheControlOffset + sizeOf (undefined :: CString)
    contentLengthOffset = contentDispositionOffset + sizeOf (undefined :: CString)
    contentMD5Offset = contentLengthOffset + sizeOf (undefined :: CULong)
    contentRangeOffset = contentMD5Offset + sizeOf (undefined :: CString)
    contentTypeOffset = contentRangeOffset + sizeOf (undefined :: FFIMaybe FFIBytesContentRange)
    eTagOffset = contentTypeOffset + sizeOf (undefined :: CString)
    lastModifiedOffset = eTagOffset + sizeOf (undefined :: CString)
  poke ptr (FFIMetadata mode cacheControl contentDisposition contentLength contentMD5 contentRange contentType eTag lastModified) = do
    pokeByteOff ptr modeOffset mode
    pokeByteOff ptr cacheControlOffset cacheControl
    pokeByteOff ptr contentDispositionOffset contentDisposition
    pokeByteOff ptr contentLengthOffset contentLength
    pokeByteOff ptr contentMD5Offset contentMD5
    pokeByteOff ptr contentRangeOffset contentRange
    pokeByteOff ptr contentTypeOffset contentType
    pokeByteOff ptr eTagOffset eTag
    pokeByteOff ptr lastModifiedOffset lastModified
   where
    modeOffset = 0
    cacheControlOffset = modeOffset + sizeOf (undefined :: CSize)
    contentDispositionOffset = cacheControlOffset + sizeOf (undefined :: FFIMaybe CString)
    contentLengthOffset = contentDispositionOffset + sizeOf (undefined :: FFIMaybe CString)
    contentMD5Offset = contentLengthOffset + sizeOf (undefined :: CULong)
    contentRangeOffset = contentMD5Offset + sizeOf (undefined :: FFIMaybe CString)
    contentTypeOffset = contentRangeOffset + sizeOf (undefined :: FFIMaybe FFIBytesContentRange)
    eTagOffset = contentTypeOffset + sizeOf (undefined :: FFIMaybe CString)
    lastModifiedOffset = eTagOffset + sizeOf (undefined :: FFIMaybe CString)

foreign import ccall "via_map_ffi"
  c_via_map_ffi ::
    CString -> Ptr CString -> Ptr CString -> CSize -> Ptr (FFIResult RawOperator) -> IO ()
foreign import ccall "&free_operator" c_free_operator :: FunPtr (Ptr RawOperator -> IO ())
foreign import ccall "free_byteslice" c_free_byteslice :: Ptr CChar -> CSize -> IO ()
foreign import ccall "blocking_read" c_blocking_read :: Ptr RawOperator -> CString -> Ptr (FFIResult ByteSlice) -> IO ()
foreign import ccall "blocking_write" c_blocking_write :: Ptr RawOperator -> CString -> Ptr CChar -> CSize -> Ptr (FFIResult ()) -> IO ()
foreign import ccall "blocking_is_exist" c_blocking_is_exist :: Ptr RawOperator -> CString -> Ptr (FFIResult CBool) -> IO ()
foreign import ccall "blocking_create_dir" c_blocking_create_dir :: Ptr RawOperator -> CString -> Ptr (FFIResult ()) -> IO ()
foreign import ccall "blocking_copy" c_blocking_copy :: Ptr RawOperator -> CString -> CString -> Ptr (FFIResult ()) -> IO ()
foreign import ccall "blocking_rename" c_blocking_rename :: Ptr RawOperator -> CString -> CString -> Ptr (FFIResult ()) -> IO ()
foreign import ccall "blocking_delete" c_blocking_delete :: Ptr RawOperator -> CString -> Ptr (FFIResult ()) -> IO ()
foreign import ccall "blocking_stat" c_blocking_stat :: Ptr RawOperator -> CString -> Ptr (FFIResult FFIMetadata) -> IO ()