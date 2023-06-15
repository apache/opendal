{-# LANGUAGE ForeignFunctionInterface #-}

module OpenDAL.FFI where

import Foreign
import Foreign.C.String
import Foreign.C.Types

data RawOperator

data FFIResult = FFIResult
  { success :: Bool
  , dataPtr :: Ptr ()
  , errorMessage :: CString
  } deriving (Show)

instance Storable FFIResult where
  sizeOf _ = sizeOf (undefined :: CSize) + sizeOf (undefined :: Ptr ()) + sizeOf (undefined :: CString)
  alignment _ = alignment (undefined :: CIntPtr)
  peek ptr = do
    s <- ((/= (0 :: CSize)) <$> peekByteOff ptr successOffset)
    d <- peekByteOff ptr dataPtrOffset
    errMsg <- peekByteOff ptr errorMessageOffset
    return $ FFIResult s d errMsg
   where
    successOffset = 0
    dataPtrOffset = sizeOf (undefined :: CSize)
    errorMessageOffset = dataPtrOffset + sizeOf (undefined :: Ptr ())
  poke ptr (FFIResult s d errMsg) = do
    pokeByteOff ptr successOffset (fromBool s :: CSize)
    pokeByteOff ptr dataPtrOffset d
    pokeByteOff ptr errorMessageOffset errMsg
   where
    successOffset = 0
    dataPtrOffset = sizeOf (undefined :: CSize)
    errorMessageOffset = dataPtrOffset + sizeOf (undefined :: Ptr ())

data ByteSlice = ByteSlice
  { bsData :: Ptr CChar
  , bsLen :: CSize
  }

instance Storable ByteSlice where
  sizeOf _ = sizeOf (undefined :: Ptr CChar) + sizeOf (undefined :: CSize)
  alignment _ = alignment (undefined :: Ptr CChar)
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