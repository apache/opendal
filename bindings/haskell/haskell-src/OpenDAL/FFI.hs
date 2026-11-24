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

module OpenDAL.FFI where

import Foreign
import Foreign.C.String
import Foreign.C.Types

alignUp :: Int -> Int -> Int
alignUp offset align
  | align <= 1 = offset
  | otherwise = ((offset + align - 1) `div` align) * align

data RawOperator

data RawLister

data RawWriter

data FFIResult a = FFIResult
  { ffiCode :: CUInt,
    dataPtr :: Ptr a,
    errorMessage :: CString
  }
  deriving (Show)

ffiResultCodeOffset :: Int
ffiResultCodeOffset = 0

ffiResultDataPtrOffset :: Int
ffiResultDataPtrOffset =
  alignUp (ffiResultCodeOffset + sizeOf (undefined :: CUInt)) (alignment (undefined :: Ptr ()))

ffiResultErrorMessageOffset :: Int
ffiResultErrorMessageOffset =
  alignUp (ffiResultDataPtrOffset + sizeOf (undefined :: Ptr ())) (alignment (undefined :: CString))

ffiResultAlignment :: Int
ffiResultAlignment =
  maximum
    [ alignment (undefined :: CUInt),
      alignment (undefined :: Ptr ()),
      alignment (undefined :: CString)
    ]

ffiResultSize :: Int
ffiResultSize =
  alignUp (ffiResultErrorMessageOffset + sizeOf (undefined :: CString)) ffiResultAlignment

instance Storable (FFIResult a) where
  sizeOf _ = ffiResultSize
  alignment _ = ffiResultAlignment
  peek ptr = do
    s <- peekByteOff ptr ffiResultCodeOffset
    d <- peekByteOff ptr ffiResultDataPtrOffset
    errMsg <- peekByteOff ptr ffiResultErrorMessageOffset
    return $ FFIResult s d errMsg
  poke ptr (FFIResult s d errMsg) = do
    pokeByteOff ptr ffiResultCodeOffset s
    pokeByteOff ptr ffiResultDataPtrOffset d
    pokeByteOff ptr ffiResultErrorMessageOffset errMsg

data ByteSlice = ByteSlice
  { bsData :: Ptr CChar,
    bsLen :: CSize
  }

byteSliceDataOffset :: Int
byteSliceDataOffset = 0

byteSliceLenOffset :: Int
byteSliceLenOffset =
  alignUp (byteSliceDataOffset + sizeOf (undefined :: Ptr CChar)) (alignment (undefined :: CSize))

byteSliceAlignment :: Int
byteSliceAlignment =
  max (alignment (undefined :: Ptr CChar)) (alignment (undefined :: CSize))

byteSliceSize :: Int
byteSliceSize =
  alignUp (byteSliceLenOffset + sizeOf (undefined :: CSize)) byteSliceAlignment

instance Storable ByteSlice where
  sizeOf _ = byteSliceSize
  alignment _ = byteSliceAlignment
  peek ptr = do
    bsDataPtr <- peekByteOff ptr byteSliceDataOffset
    len <- peekByteOff ptr byteSliceLenOffset
    return $ ByteSlice bsDataPtr len
  poke ptr (ByteSlice bsDataPtr len) = do
    pokeByteOff ptr byteSliceDataOffset bsDataPtr
    pokeByteOff ptr byteSliceLenOffset len

data FFIMetadata = FFIMetadata
  { ffiMode :: CUInt,
    ffiCacheControl :: CString,
    ffiContentDisposition :: CString,
    ffiContentLength :: Word64,
    ffiContentMD5 :: CString,
    ffiContentType :: CString,
    ffiETag :: CString,
    ffiLastModified :: CString
  }
  deriving (Show)

ffiMetadataModeOffset :: Int
ffiMetadataModeOffset = 0

ffiMetadataCacheControlOffset :: Int
ffiMetadataCacheControlOffset =
  alignUp (ffiMetadataModeOffset + sizeOf (undefined :: CUInt)) (alignment (undefined :: CString))

ffiMetadataContentDispositionOffset :: Int
ffiMetadataContentDispositionOffset =
  alignUp (ffiMetadataCacheControlOffset + sizeOf (undefined :: CString)) (alignment (undefined :: CString))

ffiMetadataContentLengthOffset :: Int
ffiMetadataContentLengthOffset =
  alignUp (ffiMetadataContentDispositionOffset + sizeOf (undefined :: CString)) (alignment (undefined :: Word64))

ffiMetadataContentMD5Offset :: Int
ffiMetadataContentMD5Offset =
  alignUp (ffiMetadataContentLengthOffset + sizeOf (undefined :: Word64)) (alignment (undefined :: CString))

ffiMetadataContentTypeOffset :: Int
ffiMetadataContentTypeOffset =
  alignUp (ffiMetadataContentMD5Offset + sizeOf (undefined :: CString)) (alignment (undefined :: CString))

ffiMetadataETagOffset :: Int
ffiMetadataETagOffset =
  alignUp (ffiMetadataContentTypeOffset + sizeOf (undefined :: CString)) (alignment (undefined :: CString))

ffiMetadataLastModifiedOffset :: Int
ffiMetadataLastModifiedOffset =
  alignUp (ffiMetadataETagOffset + sizeOf (undefined :: CString)) (alignment (undefined :: CString))

ffiMetadataAlignment :: Int
ffiMetadataAlignment =
  maximum
    [ alignment (undefined :: CUInt),
      alignment (undefined :: CString),
      alignment (undefined :: Word64)
    ]

ffiMetadataSize :: Int
ffiMetadataSize =
  alignUp (ffiMetadataLastModifiedOffset + sizeOf (undefined :: CString)) ffiMetadataAlignment

instance Storable FFIMetadata where
  sizeOf _ = ffiMetadataSize
  alignment _ = ffiMetadataAlignment
  peek ptr = do
    mode <- peekByteOff ptr ffiMetadataModeOffset
    cacheControl <- peekByteOff ptr ffiMetadataCacheControlOffset
    contentDisposition <- peekByteOff ptr ffiMetadataContentDispositionOffset
    contentLength <- peekByteOff ptr ffiMetadataContentLengthOffset
    contentMD5 <- peekByteOff ptr ffiMetadataContentMD5Offset
    contentType <- peekByteOff ptr ffiMetadataContentTypeOffset
    eTag <- peekByteOff ptr ffiMetadataETagOffset
    lastModified <- peekByteOff ptr ffiMetadataLastModifiedOffset
    return $ FFIMetadata mode cacheControl contentDisposition contentLength contentMD5 contentType eTag lastModified
  poke ptr (FFIMetadata mode cacheControl contentDisposition contentLength contentMD5 contentType eTag lastModified) = do
    pokeByteOff ptr ffiMetadataModeOffset mode
    pokeByteOff ptr ffiMetadataCacheControlOffset cacheControl
    pokeByteOff ptr ffiMetadataContentDispositionOffset contentDisposition
    pokeByteOff ptr ffiMetadataContentLengthOffset contentLength
    pokeByteOff ptr ffiMetadataContentMD5Offset contentMD5
    pokeByteOff ptr ffiMetadataContentTypeOffset contentType
    pokeByteOff ptr ffiMetadataETagOffset eTag
    pokeByteOff ptr ffiMetadataLastModifiedOffset lastModified

foreign import ccall "via_map_ffi"
  c_via_map_ffi ::
    CString ->
    Ptr CString ->
    Ptr CString ->
    CSize ->
    FunPtr (CUInt -> CString -> IO ()) ->
    Ptr (FunPtr (CUInt -> CString -> IO ())) ->
    Ptr (FFIResult RawOperator) ->
    IO ()

foreign import ccall "wrapper"
  wrapLogFn :: (CUInt -> CString -> IO ()) -> IO (FunPtr (CUInt -> CString -> IO ()))

foreign import ccall "&free_operator" c_free_operator :: FunPtr (Ptr RawOperator -> IO ())

foreign import ccall "free_operator" freeOperator :: Ptr RawOperator -> IO ()

foreign import ccall "free_byteslice" freeByteSlice :: Ptr ByteSlice -> IO ()

foreign import ccall "free_bool" freeBool :: Ptr CBool -> IO ()

foreign import ccall "free_string" freeString :: CString -> IO ()

foreign import ccall "free_metadata" freeMetadata :: Ptr FFIMetadata -> IO ()

foreign import ccall "blocking_read" c_blocking_read :: Ptr RawOperator -> CString -> Ptr (FFIResult ByteSlice) -> IO ()

foreign import ccall "blocking_write" c_blocking_write :: Ptr RawOperator -> CString -> Ptr CChar -> CSize -> Ptr (FFIResult ()) -> IO ()

foreign import ccall "blocking_is_exist" c_blocking_is_exist :: Ptr RawOperator -> CString -> Ptr (FFIResult CBool) -> IO ()

foreign import ccall "blocking_create_dir" c_blocking_create_dir :: Ptr RawOperator -> CString -> Ptr (FFIResult ()) -> IO ()

foreign import ccall "blocking_copy" c_blocking_copy :: Ptr RawOperator -> CString -> CString -> Ptr (FFIResult ()) -> IO ()

foreign import ccall "blocking_rename" c_blocking_rename :: Ptr RawOperator -> CString -> CString -> Ptr (FFIResult ()) -> IO ()

foreign import ccall "blocking_delete" c_blocking_delete :: Ptr RawOperator -> CString -> Ptr (FFIResult ()) -> IO ()

foreign import ccall "blocking_stat" c_blocking_stat :: Ptr RawOperator -> CString -> Ptr (FFIResult FFIMetadata) -> IO ()

foreign import ccall "blocking_list" c_blocking_list :: Ptr RawOperator -> CString -> Ptr (FFIResult RawLister) -> IO ()

foreign import ccall "blocking_scan" c_blocking_scan :: Ptr RawOperator -> CString -> Ptr (FFIResult RawLister) -> IO ()

foreign import ccall "lister_next" c_lister_next :: Ptr RawLister -> Ptr (FFIResult CChar) -> IO ()

foreign import ccall "&free_lister" c_free_lister :: FunPtr (Ptr RawLister -> IO ())

foreign import ccall "free_lister" freeLister :: Ptr RawLister -> IO ()

foreign import ccall "operator_info" c_operator_info :: Ptr RawOperator -> Ptr (FFIResult CChar) -> IO ()

foreign import ccall "blocking_writer" c_blocking_writer :: Ptr RawOperator -> CString -> Ptr (FFIResult RawWriter) -> IO ()

foreign import ccall "blocking_writer_append" c_blocking_writer_append :: Ptr RawOperator -> CString -> Ptr (FFIResult RawWriter) -> IO ()

foreign import ccall "writer_write" c_writer_write :: Ptr RawWriter -> Ptr CChar -> CSize -> Ptr (FFIResult ()) -> IO ()

foreign import ccall "writer_close" c_writer_close :: Ptr RawWriter -> Ptr (FFIResult FFIMetadata) -> IO ()

foreign import ccall "&free_writer" c_free_writer :: FunPtr (Ptr RawWriter -> IO ())

foreign import ccall "free_writer" freeWriter :: Ptr RawWriter -> IO ()

foreign import ccall "blocking_remove_all" c_blocking_remove_all :: Ptr RawOperator -> CString -> Ptr (FFIResult ()) -> IO ()
