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

module OpenDAL (
  Operator,
  newOp,
  readOp,
  writeOp,
  isExistOp,
  createDirOp,
  copyOp,
  renameOp,
  deleteOp,
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Foreign
import Foreign.C.String
import Foreign.C.Types (CChar)
import OpenDAL.FFI

newtype Operator = Operator (ForeignPtr RawOperator)

data ErrorCode
  = FFIError
  | Unexpected
  | Unsupported
  | ConfigInvalid
  | NotFound
  | PermissionDenied
  | IsADirectory
  | NotADirectory
  | AlreadyExists
  | RateLimited
  | IsSameFile
  deriving (Eq, Show)

data OpenDALError = OpenDALError {errorCode :: ErrorCode, message :: String}
  deriving (Eq, Show)

byteSliceToByteString :: ByteSlice -> IO ByteString
byteSliceToByteString (ByteSlice bsDataPtr len) = BS.packCStringLen (bsDataPtr, fromIntegral len)

parseErrorCode :: Int -> ErrorCode
parseErrorCode 1 = FFIError
parseErrorCode 2 = Unexpected
parseErrorCode 3 = Unsupported
parseErrorCode 4 = ConfigInvalid
parseErrorCode 5 = NotFound
parseErrorCode 6 = PermissionDenied
parseErrorCode 7 = IsADirectory
parseErrorCode 8 = NotADirectory
parseErrorCode 9 = AlreadyExists
parseErrorCode 10 = RateLimited
parseErrorCode 11 = IsSameFile
parseErrorCode _ = FFIError

-- | Create a new Operator.
newOp :: String -> HashMap String String -> IO (Either OpenDALError Operator)
newOp scheme hashMap = do
  let keysAndValues = HashMap.toList hashMap
  withCString scheme $ \cScheme ->
    withMany withCString (map fst keysAndValues) $ \cKeys ->
      withMany withCString (map snd keysAndValues) $ \cValues ->
        allocaArray (length keysAndValues) $ \cKeysPtr ->
          allocaArray (length keysAndValues) $ \cValuesPtr ->
            alloca $ \ffiResultPtr -> do
              pokeArray cKeysPtr cKeys
              pokeArray cValuesPtr cValues
              c_via_map_ffi cScheme cKeysPtr cValuesPtr (fromIntegral $ length keysAndValues) ffiResultPtr
              ffiResult <- peek ffiResultPtr
              if ffiCode ffiResult == 0
                then do
                  op <- Operator <$> (newForeignPtr c_free_operator $ castPtr $ dataPtr ffiResult)
                  return $ Right op
                else do
                  let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
                  errMsg <- peekCString (errorMessage ffiResult)
                  return $ Left $ OpenDALError code errMsg

readOp :: Operator -> String -> IO (Either OpenDALError ByteString)
readOp (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_read opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          byteslice <- peek (castPtr $ dataPtr ffiResult)
          byte <- byteSliceToByteString byteslice
          c_free_byteslice (bsData byteslice) (bsLen byteslice)
          return $ Right byte
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

writeOp :: Operator -> String -> ByteString -> IO (Either OpenDALError ())
writeOp (Operator op) path byte = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    BS.useAsCStringLen byte $ \(cByte, len) ->
      alloca $ \ffiResultPtr -> do
        c_blocking_write opptr cPath cByte (fromIntegral len) ffiResultPtr
        ffiResult <- peek ffiResultPtr
        if ffiCode ffiResult == 0
          then return $ Right ()
          else do
            let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
            errMsg <- peekCString (errorMessage ffiResult)
            return $ Left $ OpenDALError code errMsg

isExistOp :: Operator -> String -> IO (Either OpenDALError Bool)
isExistOp (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_is_exist opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          -- For Bool type, the memory layout is different between C and Haskell.
          val <- peek ((castPtr $ dataPtr ffiResult) :: Ptr CChar)
          let isExist = val /= 0
          return $ Right isExist
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

createDirOp :: Operator -> String -> IO (Either OpenDALError ())
createDirOp (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_create_dir opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then return $ Right ()
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

copyOp :: Operator -> String -> String -> IO (Either OpenDALError ())
copyOp (Operator op) srcPath dstPath = withForeignPtr op $ \opptr ->
  withCString srcPath $ \cSrcPath ->
    withCString dstPath $ \cDstPath ->
      alloca $ \ffiResultPtr -> do
        c_blocking_copy opptr cSrcPath cDstPath ffiResultPtr
        ffiResult <- peek ffiResultPtr
        if ffiCode ffiResult == 0
          then return $ Right ()
          else do
            let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
            errMsg <- peekCString (errorMessage ffiResult)
            return $ Left $ OpenDALError code errMsg

renameOp :: Operator -> String -> String -> IO (Either OpenDALError ())
renameOp (Operator op) srcPath dstPath = withForeignPtr op $ \opptr ->
  withCString srcPath $ \cSrcPath ->
    withCString dstPath $ \cDstPath ->
      alloca $ \ffiResultPtr -> do
        c_blocking_rename opptr cSrcPath cDstPath ffiResultPtr
        ffiResult <- peek ffiResultPtr
        if ffiCode ffiResult == 0
          then return $ Right ()
          else do
            let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
            errMsg <- peekCString (errorMessage ffiResult)
            return $ Left $ OpenDALError code errMsg

deleteOp :: Operator -> String -> IO (Either OpenDALError ())
deleteOp (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_delete opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then return $ Right ()
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg