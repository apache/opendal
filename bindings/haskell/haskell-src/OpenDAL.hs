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

-- |
-- Module      : OpenDAL
-- Description : Haskell bindings for Apache OpenDAL
-- Copyright   : (c) 2023 Apache OpenDAL
-- License     : Apache-2.0
-- Maintainer  : OpenDAL Contributors <dev@opendal.apache.org>"
-- Stability   : experimental
-- Portability : non - portable (GHC extensions)
--
-- This module provides Haskell bindings for Apache OpenDAL.
module OpenDAL
  ( -- * Operator APIs

    -- ** Types
    OperatorConfig (..),
    Operator,
    Lister,
    Writer,
    WriterOption (..),
    OpenDALError (..),
    ErrorCode (..),
    EntryMode (..),
    Metadata (..),
    OperatorT (..),
    MonadOperation (..),

    -- ** Functions
    runOp,
    newOperator,

    -- * Writer Option Functions
    defaultWriterOption,
    appendWriterOption,

    -- * Lister APIs
    nextLister,

    -- * Writer APIs
    writerOpRaw,
    writerWrite,
    writerClose,

    -- * Operator Raw APIs
    -- $raw-operations
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
    operatorInfoRaw,
    removeAllOpRaw,
  )
where

import Colog (LogAction, Message, Msg (Msg), (<&))
import Control.Exception (finally, mask_, onException)
import Control.Monad.Except (ExceptT, MonadError, MonadTrans, runExceptT, throwError)
import Control.Monad.Reader (MonadIO, MonadReader, ReaderT, ask, liftIO, runReaderT)
import Control.Monad.Trans (MonadTrans (lift))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Maybe (fromMaybe)
import Data.String (IsString (fromString))
import Data.Text (pack)
import Data.Time (UTCTime, parseTimeM, zonedTimeToUTC)
import Data.Time.Format (defaultTimeLocale)
import Foreign
import Foreign.C.String (CString, peekCString)
import qualified GHC.Foreign as GHCForeign
import GHC.IO.Encoding (utf8)
import GHC.Stack (emptyCallStack)
import OpenDAL.FFI

-- | `OperatorConfig` is the configuration for an `Operator`.
-- We recommend using `OverloadedStrings` to construct a default config.
--
-- For example:
--
-- default config
--
-- @
-- newOperator "memory"
-- @
--
-- custom services config
--
-- @
-- newOperator "memory" {ocConfig = HashMap.fromList [("root", "/tmp")]}
-- @
--
-- enable logging
--
-- @
-- newOperator "memory" {ocLogAction = Just simpleMessageAction}
-- @
data OperatorConfig = OperatorConfig
  { -- | The scheme of the operator. For example, "s3" or "gcs".
    ocScheme :: String,
    -- | The config for the operator services. For example, "endpoint" and "access_key_id" for s3. The config is same as rust core.
    ocConfig :: HashMap String String,
    -- | The log action for the operator. If it's `Nothing`, the operator won't enable logging.
    ocLogAction :: Maybe (LogAction IO Message)
  }

instance IsString OperatorConfig where
  fromString s = OperatorConfig s HashMap.empty Nothing

-- | `Operator` is the entry for all public blocking APIs.
-- Create an `Operator` with `newOperator`.
newtype Operator = Operator (ForeignPtr RawOperator)

-- | `Lister` is designed to list entries at given path in a blocking manner.
-- Users can construct Lister by `listOp` or `scanOp`.
newtype Lister = Lister (ForeignPtr RawLister)

-- | `Writer` is designed to write bytes into given path.
newtype Writer = Writer (ForeignPtr RawWriter)

-- | Options for creating a Writer.
data WriterOption = WriterOption
  { -- | Whether to append to the file instead of overwriting it.
    woAppend :: Bool
  }
  deriving (Eq, Show)

-- | Represents the possible error codes that can be returned by OpenDAL.
data ErrorCode
  = -- | An error occurred in the FFI layer.
    FFIError
  | -- | OpenDAL don't know what happened here, and no actions other than just returning it back. For example, s3 returns an internal service error.
    Unexpected
  | -- | Underlying service doesn't support this operation.
    Unsupported
  | -- | The config for backend is invalid.
    ConfigInvalid
  | -- | The given path is not found.
    NotFound
  | -- | The given path doesn't have enough permission for this operation.
    PermissionDenied
  | -- | The given path is a directory.
    IsADirectory
  | -- | The given path is not a directory.
    NotADirectory
  | -- | The given path already exists thus we failed to the specified operation on it.
    AlreadyExists
  | -- | Requests that sent to this path is over the limit, please slow down.
    RateLimited
  | -- | The given file paths are same.
    IsSameFile
  | -- | The operation conflicts with the current or transitional state of the resource.
    Conflict
  | -- | A condition supplied through the OpenDAL operation evaluated to false.
    ConditionNotMatch
  | -- | The requested content range cannot be satisfied.
    RangeNotSatisfied
  deriving (Eq, Show)

-- | Represents an error that can occur when using OpenDAL.
data OpenDALError = OpenDALError
  { -- | The error code.
    errorCode :: ErrorCode,
    -- | The error message.
    message :: String
  }
  deriving (Eq, Show)

-- | Represents the mode of an entry in a storage system (e.g., file or directory).
data EntryMode = File | Dir | Unknown deriving (Eq, Show)

-- | Represents metadata for an entry in a storage system.
data Metadata = Metadata
  { -- | The mode of the entry.
    mMode :: EntryMode,
    -- | The cache control of the entry.
    mCacheControl :: Maybe String,
    -- | The content disposition of the entry.
    mContentDisposition :: Maybe String,
    -- | The content length of the entry.
    mContentLength :: Integer,
    -- | The content MD5 of the entry.
    mContentMD5 :: Maybe String,
    -- | The content type of the entry.
    mContentType :: Maybe String,
    -- | The ETag of the entry.
    mETag :: Maybe String,
    -- | The last modified time of the entry.
    mLastModified :: Maybe UTCTime
  }
  deriving (Eq, Show)

-- | @newtype@ wrapper 'ReaderT' that keeps 'Operator' in its context.
newtype OperatorT m a = OperatorT
  {runOperatorT :: ReaderT Operator (ExceptT OpenDALError m) a}
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader Operator, MonadError OpenDALError)

instance MonadTrans OperatorT where
  lift = OperatorT . lift . lift

-- | A type class for monads that can perform OpenDAL operations.
class (Monad m) => MonadOperation m where
  -- | Read the whole path into a bytes.
  readOp :: String -> m ByteString

  -- | Write bytes into given path.
  writeOp :: String -> ByteString -> m ()

  -- | Check if this path exists or not.
  isExistOp :: String -> m Bool

  -- | Create a dir at given path.
  createDirOp :: String -> m ()

  -- | Copy a file from srcPath to dstPath.
  copyOp :: String -> String -> m ()

  -- | Rename a file from srcPath to dstPath.
  renameOp :: String -> String -> m ()

  -- | Delete given path.
  deleteOp :: String -> m ()

  -- | Get given path's metadata without cache directly.
  statOp :: String -> m Metadata

  -- | List current dir path.
  -- This function will create a new handle to list entries.
  -- An error will be returned if path doesn't end with /.
  listOp :: String -> m Lister

  -- | List dir in flat way.
  -- Also, this function can be used to list a prefix.
  -- An error will be returned if given path doesn't end with /.
  scanOp :: String -> m Lister

  -- | Remove all files and directories recursively.
  removeAllOp :: String -> m ()

instance (MonadIO m) => MonadOperation (OperatorT m) where
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
  removeAllOp path = do
    op <- ask
    result <- liftIO $ removeAllOpRaw op path
    either throwError return result

-- helper functions

byteSliceToByteString :: ByteSlice -> IO ByteString
byteSliceToByteString (ByteSlice bsDataPtr len)
  | bsDataPtr == nullPtr || len == 0 = return BS.empty
  | otherwise = BS.packCStringLen (bsDataPtr, fromIntegral len)

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
parseErrorCode 12 = Conflict
parseErrorCode 13 = ConditionNotMatch
parseErrorCode 14 = RangeNotSatisfied
parseErrorCode _ = FFIError

parseEntryMode :: Int -> EntryMode
parseEntryMode 0 = File
parseEntryMode 1 = Dir
parseEntryMode _ = Unknown

withUtf8CString :: String -> (CString -> IO a) -> IO a
withUtf8CString = GHCForeign.withCString utf8

peekUtf8CString :: CString -> IO String
peekUtf8CString = GHCForeign.peekCString utf8

maybePeekCString :: CString -> IO (Maybe String)
maybePeekCString value | value == nullPtr = return Nothing
maybePeekCString value = Just <$> peekUtf8CString value

-- | Peek a Rust-owned `CString` and release it through Rust's `free_string`.
-- The pointer is always released, even when `peekUtf8CString` throws.
-- Every byte allocated by Rust must be freed by a Rust function; calling C
-- `free` on it is an allocator mismatch.
parseOwnedCString :: CString -> IO (Maybe String)
parseOwnedCString value
  | value == nullPtr = return Nothing
  | otherwise = (Just <$> peekUtf8CString value) `finally` freeString value

-- | Convert a failed `FFIResult` into an `OpenDALError`, releasing the
-- Rust-owned `errorMessage` buffer through `free_string`.
parseError :: FFIResult a -> IO OpenDALError
parseError ffiResult = do
  let code = parseErrorCode $ fromIntegral $ ffiCode ffiResult
  errMsg <- fromMaybe "" <$> parseOwnedCString (errorMessage ffiResult)
  return $ OpenDALError code errMsg

parseTime :: String -> Maybe UTCTime
parseTime time = zonedTimeToUTC <$> parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%Q%z" time

-- | Peek the fields of a Rust `Metadata`. The caller releases the whole
-- struct through `free_metadata`, so fields are peeked but never freed here.
parseFFIMetadata :: FFIMetadata -> IO Metadata
parseFFIMetadata (FFIMetadata mode cacheControl contentDisposition contentLength contentMD5 contentType eTag lastModified) = do
  let mode' = parseEntryMode $ fromIntegral mode
  cacheControl' <- maybePeekCString cacheControl
  contentDisposition' <- maybePeekCString contentDisposition
  let contentLength' = toInteger contentLength
  contentMD5' <- maybePeekCString contentMD5
  contentType' <- maybePeekCString contentType
  eTag' <- maybePeekCString eTag
  lastModified' <- (>>= parseTime) <$> maybePeekCString lastModified
  return $
    Metadata
      { mMode = mode',
        mCacheControl = cacheControl',
        mContentDisposition = contentDisposition',
        mContentLength = contentLength',
        mContentMD5 = contentMD5',
        mContentType = contentType',
        mETag = eTag',
        mLastModified = lastModified'
      }

-- Exported functions

-- | Default WriterOption for writing (overwriting).
defaultWriterOption :: WriterOption
defaultWriterOption = WriterOption {woAppend = False}

-- | WriterOption for appending to a file.
appendWriterOption :: WriterOption
appendWriterOption = WriterOption {woAppend = True}

-- |  Runner for 'OperatorT' monad.
-- This function will run given 'OperatorT' monad with given 'Operator'.
--
-- Let's see an example:
--
-- @
-- operation :: MonadOperation m => m ()
-- operation = __do__
--    writeOp op "key1" "value1"
--    writeOp op "key2" "value2"
--    value1 <- readOp op "key1"
--    value2 <- readOp op "key2"
-- @
--
-- You can run this operation with 'runOp' function:
--
-- @
-- runOp operator operation
-- @
runOp :: Operator -> OperatorT m a -> m (Either OpenDALError a)
runOp op = runExceptT . flip runReaderT op . runOperatorT
{-# INLINE runOp #-}

-- | Creates a new OpenDAL operator via `OperatorConfig`.
newOperator :: OperatorConfig -> IO (Either OpenDALError Operator)
newOperator (OperatorConfig scheme hashMap maybeLogger) = do
  let keysAndValues = HashMap.toList hashMap
  withUtf8CString scheme $ \cScheme ->
    withMany withUtf8CString (map fst keysAndValues) $ \cKeys ->
      withMany withUtf8CString (map snd keysAndValues) $ \cValues ->
        allocaArray (length keysAndValues) $ \cKeysPtr ->
          allocaArray (length keysAndValues) $ \cValuesPtr ->
            alloca $ \ffiResultPtr -> mask_ $ do
              -- Once Rust registers the logger the callback is owned by the
              -- global Rust logger for the rest of the process. We
              -- deliberately never call freeHaskellFunPtr: if registration
              -- succeeded, freeing it would leave a dangling pointer.
              logFnPtr <- case maybeLogger of
                Just logger -> wrapLogFn (logFn logger)
                Nothing -> return nullFunPtr
              pokeArray cKeysPtr cKeys
              pokeArray cValuesPtr cValues
              c_via_map_ffi cScheme cKeysPtr cValuesPtr (fromIntegral $ length keysAndValues) logFnPtr ffiResultPtr
              ffiResult <- peek ffiResultPtr
              if ffiCode ffiResult == 0
                then do
                  op <-
                    Operator
                      <$> newForeignPtr c_free_operator (dataPtr ffiResult)
                      `onException` freeOperator (dataPtr ffiResult)
                  return $ Right op
                else Left <$> parseError ffiResult
  where
    logFn logger enumSeverity cStr = do
      str <- peekCString cStr
      logger <& Msg (toEnum (fromIntegral enumSeverity)) emptyCallStack (pack str)

-- $raw-operations
-- Functions for performing raw OpenDAL operations are defined below.
-- These functions are not meant to be used directly in most cases.
-- Instead, use the high-level interface provided by the 'MonadOperation' type class.

-- | Read the whole path into a bytes.
readOpRaw :: Operator -> String -> IO (Either OpenDALError ByteString)
readOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_read opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          byte <-
            (peek (dataPtr ffiResult) >>= byteSliceToByteString)
              `finally` freeByteSlice (dataPtr ffiResult)
          return $ Right byte
        else Left <$> parseError ffiResult

-- | Write bytes into given path.
writeOpRaw :: Operator -> String -> ByteString -> IO (Either OpenDALError ())
writeOpRaw (Operator op) path byte = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    BS.useAsCStringLen byte $ \(cByte, len) ->
      alloca $ \ffiResultPtr -> mask_ $ do
        c_blocking_write opptr cPath cByte (fromIntegral len) ffiResultPtr
        ffiResult <- peek ffiResultPtr
        if ffiCode ffiResult == 0
          then return $ Right ()
          else Left <$> parseError ffiResult

-- | Check if this path exists or not.
isExistOpRaw :: Operator -> String -> IO (Either OpenDALError Bool)
isExistOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_is_exist opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          val <- peek (dataPtr ffiResult) `finally` freeBool (dataPtr ffiResult)
          let isExist = val /= 0
          return $ Right isExist
        else Left <$> parseError ffiResult

-- | Create a dir at given path.
createDirOpRaw :: Operator -> String -> IO (Either OpenDALError ())
createDirOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_create_dir opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then return $ Right ()
        else Left <$> parseError ffiResult

-- | Copy a file from srcPath to dstPath.
copyOpRaw :: Operator -> String -> String -> IO (Either OpenDALError ())
copyOpRaw (Operator op) srcPath dstPath = withForeignPtr op $ \opptr ->
  withUtf8CString srcPath $ \cSrcPath ->
    withUtf8CString dstPath $ \cDstPath ->
      alloca $ \ffiResultPtr -> mask_ $ do
        c_blocking_copy opptr cSrcPath cDstPath ffiResultPtr
        ffiResult <- peek ffiResultPtr
        if ffiCode ffiResult == 0
          then return $ Right ()
          else Left <$> parseError ffiResult

-- | Rename a file from srcPath to dstPath.
renameOpRaw :: Operator -> String -> String -> IO (Either OpenDALError ())
renameOpRaw (Operator op) srcPath dstPath = withForeignPtr op $ \opptr ->
  withUtf8CString srcPath $ \cSrcPath ->
    withUtf8CString dstPath $ \cDstPath ->
      alloca $ \ffiResultPtr -> mask_ $ do
        c_blocking_rename opptr cSrcPath cDstPath ffiResultPtr
        ffiResult <- peek ffiResultPtr
        if ffiCode ffiResult == 0
          then return $ Right ()
          else Left <$> parseError ffiResult

-- | Delete given path.
deleteOpRaw :: Operator -> String -> IO (Either OpenDALError ())
deleteOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_delete opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then return $ Right ()
        else Left <$> parseError ffiResult

-- | Get given path's metadata without cache directly.
statOpRaw :: Operator -> String -> IO (Either OpenDALError Metadata)
statOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_stat opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          metadata <-
            (peek (dataPtr ffiResult) >>= parseFFIMetadata)
              `finally` freeMetadata (dataPtr ffiResult)
          return $ Right metadata
        else Left <$> parseError ffiResult

-- | List current dir path.
-- This function will create a new handle to list entries.
-- An error will be returned if path doesn't end with /.
listOpRaw :: Operator -> String -> IO (Either OpenDALError Lister)
listOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_list opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          lister <-
            Lister
              <$> newForeignPtr c_free_lister (dataPtr ffiResult)
              `onException` freeLister (dataPtr ffiResult)
          return $ Right lister
        else Left <$> parseError ffiResult

-- | List dir in flat way.
-- Also, this function can be used to list a prefix.
-- An error will be returned if given path doesn't end with /.
scanOpRaw :: Operator -> String -> IO (Either OpenDALError Lister)
scanOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_scan opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          lister <-
            Lister
              <$> newForeignPtr c_free_lister (dataPtr ffiResult)
              `onException` freeLister (dataPtr ffiResult)
          return $ Right lister
        else Left <$> parseError ffiResult

-- | Get operator info (scheme).
operatorInfoRaw :: Operator -> IO (Either OpenDALError String)
operatorInfoRaw (Operator op) = withForeignPtr op $ \opptr ->
  alloca $ \ffiResultPtr -> mask_ $ do
    c_operator_info opptr ffiResultPtr
    ffiResult <- peek ffiResultPtr
    if ffiCode ffiResult == 0
      then Right . fromMaybe "" <$> parseOwnedCString (dataPtr ffiResult)
      else Left <$> parseError ffiResult

-- | Get next entry path from `Lister`.
nextLister :: Lister -> IO (Either OpenDALError (Maybe String))
nextLister (Lister lister) = withForeignPtr lister $ \listerptr ->
  alloca $ \ffiResultPtr -> mask_ $ do
    c_lister_next listerptr ffiResultPtr
    ffiResult <- peek ffiResultPtr
    if ffiCode ffiResult == 0
      then Right <$> parseOwnedCString (dataPtr ffiResult)
      else Left <$> parseError ffiResult


-- | Writes bytes into given writer.
writerWrite :: Writer -> ByteString -> IO (Either OpenDALError ())
writerWrite (Writer writer) byte = withForeignPtr writer $ \writerptr ->
  BS.useAsCStringLen byte $ \(cByte, len) ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_writer_write writerptr cByte (fromIntegral len) ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then return $ Right ()
        else Left <$> parseError ffiResult

-- | Closes given writer.
writerClose :: Writer -> IO (Either OpenDALError Metadata)
writerClose (Writer writer) = withForeignPtr writer $ \writerptr ->
  alloca $ \ffiResultPtr -> mask_ $ do
    c_writer_close writerptr ffiResultPtr
    ffiResult <- peek ffiResultPtr
    if ffiCode ffiResult == 0
      then do
        metadata <-
          (peek (dataPtr ffiResult) >>= parseFFIMetadata)
            `finally` freeMetadata (dataPtr ffiResult)
        return $ Right metadata
      else Left <$> parseError ffiResult

-- | Create a new writer for given path with specified options.
writerOpRaw :: Operator -> String -> WriterOption -> IO (Either OpenDALError Writer)
writerOpRaw (Operator op) path writerOption = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      if woAppend writerOption
        then c_blocking_writer_append opptr cPath ffiResultPtr
        else c_blocking_writer opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then do
          writer <-
            Writer
              <$> newForeignPtr c_free_writer (dataPtr ffiResult)
              `onException` freeWriter (dataPtr ffiResult)
          return $ Right writer
        else Left <$> parseError ffiResult



-- | Remove all files and directories recursively.
removeAllOpRaw :: Operator -> String -> IO (Either OpenDALError ())
removeAllOpRaw (Operator op) path = withForeignPtr op $ \opptr ->
  withUtf8CString path $ \cPath ->
    alloca $ \ffiResultPtr -> mask_ $ do
      c_blocking_remove_all opptr cPath ffiResultPtr
      ffiResult <- peek ffiResultPtr
      if ffiCode ffiResult == 0
        then return $ Right ()
        else Left <$> parseError ffiResult
