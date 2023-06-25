{-# LANGUAGE FlexibleInstances #-}
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
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}

module OpenDAL (
  Operator,
  Lister,
  OpenDALError (..),
  ErrorCode (..),
  EntryMode (..),
  Metadata (..),
  OpMonad,
  MonadOperation (..),
  runOp,
  newOp,
  readOpRaw,
  writeOpRaw,
  isExistOpRaw,
  createDirOpRaw,
  copyOpRaw,
  renameOpRaw,
  deleteOpRaw,
  statOpRaw,
  listOpRaw,
  scanOpRaw,
  nextLister,
) where

import Control.Monad.Except (ExceptT, runExceptT, throwError)
import Control.Monad.Reader (ReaderT, ask, liftIO, runReaderT)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Time (UTCTime, parseTimeM, zonedTimeToUTC)
import Data.Time.Format (defaultTimeLocale)
import Foreign
import Foreign.C.String
import OpenDAL.FFI

newtype Operator = Operator (ForeignPtr RawOperator)

newtype Lister = Lister (ForeignPtr RawLister)

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

data EntryMode = File | Dir | Unknown deriving (Eq, Show)

data Metadata = Metadata
  { mMode :: EntryMode
  , mCacheControl :: Maybe String
  , mContentDisposition :: Maybe String
  , mContentLength :: Integer
  , mContentMD5 :: Maybe String
  , mContentType :: Maybe String
  , mETag :: Maybe String
  , mLastModified :: Maybe UTCTime
  }
  deriving (Eq, Show)

type OpMonad = ReaderT Operator (ExceptT OpenDALError IO)

class (Monad m) => MonadOperation m where
  readOp :: String -> m ByteString
  writeOp :: String -> ByteString -> m ()
  isExistOp :: String -> m Bool
  createDirOp :: String -> m ()
  copyOp :: String -> String -> m ()
  renameOp :: String -> String -> m ()
  deleteOp :: String -> m ()
  statOp :: String -> m Metadata
  listOp :: String -> m Lister
  scanOp :: String -> m Lister

instance MonadOperation OpMonad where
  readOp path = do
    op <- ask
    result <- liftIO $ readOpRaw op path
    either throwError return result
  writeOp path byte = do
    op <- ask
    result <- liftIO $ writeOpRaw op path byte
    either throwError return result
  isExistOp path = do
    op <- ask
    result <- liftIO $ isExistOpRaw op path
    either throwError return result
  createDirOp path = do
    op <- ask
    result <- liftIO $ createDirOpRaw op path
    either throwError return result
  copyOp src dst = do
    op <- ask
    result <- liftIO $ copyOpRaw op src dst
    either throwError return result
  renameOp src dst = do
    op <- ask
    result <- liftIO $ renameOpRaw op src dst
    either throwError return result
  deleteOp path = do
    op <- ask
    result <- liftIO $ deleteOpRaw op path
    either throwError return result
  statOp path = do
    op <- ask
    result <- liftIO $ statOpRaw op path
    either throwError return result
  listOp path = do
    op <- ask
    result <- liftIO $ listOpRaw op path
    either throwError return result
  scanOp path = do
    op <- ask
    result <- liftIO $ scanOpRaw op path
    either throwError return result

-- helper functions

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

parseEntryMode :: Int -> EntryMode
parseEntryMode 0 = File
parseEntryMode 1 = Dir
parseEntryMode _ = Unknown

parseCString :: CString -> IO (Maybe String)
parseCString value | value == nullPtr = return Nothing
parseCString value = do
  value' <- peekCString value
  free value
  return $ Just value'

parseTime :: String -> Maybe UTCTime
parseTime time = zonedTimeToUTC <$> parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%Q%z" time

parseFFIMetadata :: FFIMetadata -> IO Metadata
parseFFIMetadata (FFIMetadata mode cacheControl contentDisposition contentLength contentMD5 contentType eTag lastModified) = do
  let mode' = parseEntryMode $ fromIntegral mode
  cacheControl' <- parseCString cacheControl
  contentDisposition' <- parseCString contentDisposition
  let contentLength' = toInteger contentLength
  contentMD5' <- parseCString contentMD5
  contentType' <- parseCString contentType
  eTag' <- parseCString eTag
  lastModified' <- (>>= parseTime) <$> parseCString lastModified
  return $
    Metadata
      { mMode = mode'
      , mCacheControl = cacheControl'
      , mContentDisposition = contentDisposition'
      , mContentLength = contentLength'
      , mContentMD5 = contentMD5'
      , mContentType = contentType'
      , mETag = eTag'
      , mLastModified = lastModified'
      }

-- Exported functions

runOp :: Operator -> OpMonad a -> IO (Either OpenDALError a)
runOp operator op = runExceptT $ runReaderT op operator

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
                  op <- Operator <$> (newForeignPtr c_free_operator $ dataPtr ffiResult)
                  return $ Right op
                else do
                  let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
                  errMsg <- peekCString (errorMessage ffiResult)
                  return $ Left $ OpenDALError code errMsg

readOpRaw :: Operator -> String -> IO (Either OpenDALError ByteString)
readOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_read opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          byteslice <- peek $ dataPtr ffiResult
          byte <- byteSliceToByteString byteslice
          c_free_byteslice (bsData byteslice) (bsLen byteslice)
          return $ Right byte
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

writeOpRaw :: Operator -> String -> ByteString -> IO (Either OpenDALError ())
writeOpRaw (Operator op) path byte = withForeignPtr op $ \opptr ->
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

isExistOpRaw :: Operator -> String -> IO (Either OpenDALError Bool)
isExistOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_is_exist opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          val <- peek $ dataPtr ffiResult
          let isExist = val /= 0
          return $ Right isExist
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

createDirOpRaw :: Operator -> String -> IO (Either OpenDALError ())
createDirOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
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

copyOpRaw :: Operator -> String -> String -> IO (Either OpenDALError ())
copyOpRaw (Operator op) srcPath dstPath = withForeignPtr op $ \opptr ->
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

renameOpRaw :: Operator -> String -> String -> IO (Either OpenDALError ())
renameOpRaw (Operator op) srcPath dstPath = withForeignPtr op $ \opptr ->
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

deleteOpRaw :: Operator -> String -> IO (Either OpenDALError ())
deleteOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
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

statOpRaw :: Operator -> String -> IO (Either OpenDALError Metadata)
statOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_stat opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          ffimatadata <- peek $ dataPtr ffiResult
          metadata <- parseFFIMetadata ffimatadata
          return $ Right metadata
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

listOpRaw :: Operator -> String -> IO (Either OpenDALError Lister)
listOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_list opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          ffilister <- peek $ dataPtr ffiResult
          lister <- Lister <$> (newForeignPtr c_free_lister ffilister)
          return $ Right lister
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

scanOpRaw :: Operator -> String -> IO (Either OpenDALError Lister)
scanOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withCString path $ \cPath ->
    alloca $ \ffiResultPtr -> do
      c_blocking_scan opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          ffilister <- peek $ dataPtr ffiResult
          lister <- Lister <$> (newForeignPtr c_free_lister ffilister)
          return $ Right lister
        else do
          let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
          errMsg <- peekCString (errorMessage ffiResult)
          return $ Left $ OpenDALError code errMsg

nextLister :: Lister -> IO (Either OpenDALError (Maybe String))
nextLister (Lister lister) = withForeignPtr lister $ \listerptr ->
  alloca $ \ffiResultPtr -> do
    c_lister_next listerptr ffiResultPtr
    ffiResult <- peek ffiResultPtr
    if ffiCode ffiResult == 0
      then do
        val <- peek $ dataPtr ffiResult
        Right <$> parseCString val
      else do
        let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
        errMsg <- peekCString (errorMessage ffiResult)
        return $ Left $ OpenDALError code errMsg