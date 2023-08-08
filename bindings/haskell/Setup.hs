import Control.Monad
import Data.Maybe
import qualified Distribution.PackageDescription as PD
import Distribution.Simple
import Distribution.Simple.LocalBuildInfo
import Distribution.Simple.Setup
import Distribution.Simple.Utils
import Distribution.System
import System.Directory
import System.Environment
import System.Process

main :: IO ()
main =
  defaultMainWithHooks
    simpleUserHooks
      { confHook = rustConfHook,
        buildHook = rustBuildHook
      }

rustConfHook :: (PD.GenericPackageDescription, PD.HookedBuildInfo) -> ConfigFlags -> IO LocalBuildInfo
rustConfHook (description, buildInfo) flags = do
  localBuildInfo <- confHook simpleUserHooks (description, buildInfo) flags
  let packageDescription = localPkgDescr localBuildInfo
      library = fromJust $ PD.library packageDescription
      libraryBuildInfo = PD.libBuildInfo library
  dir <- getLibDir
  return
    localBuildInfo
      { localPkgDescr =
          packageDescription
            { PD.library =
                Just $
                  library
                    { PD.libBuildInfo =
                        libraryBuildInfo
                          { PD.extraLibDirs = dir : PD.extraLibDirs libraryBuildInfo
                          }
                    }
            }
      }

rustBuildHook :: PD.PackageDescription -> LocalBuildInfo -> UserHooks -> BuildFlags -> IO ()
rustBuildHook pkg_descr lbi hooks flags = do
  putStrLn "Building Rust code..."
  rawSystemExit (fromFlag $ buildVerbosity flags) "cargo" ["build", "--release"]
  createHSLink
  putStrLn "Build Rust code success!"
  buildHook simpleUserHooks pkg_descr lbi hooks flags
  where
    createHSLink = do
      dir <- getLibDir
      ghcVersion <- init <$> readProcess "ghc" ["--numeric-version"] ""
      let srcPath = dir ++ "/libopendal_hs." ++ getDynamicLibExtension lbi
      let destPath = dir ++ "/libopendal_hs-ghc" ++ ghcVersion ++ "." ++ getDynamicLibExtension lbi
      exist <- doesFileExist destPath
      when (not exist) $ createFileLink srcPath destPath

getLibDir :: IO String
getLibDir = do
  cargoPath <- readProcess "cargo" ["locate-project", "--workspace", "--message-format=plain"] ""
  let dir = take (length cargoPath - 11) cargoPath -- <dir>/Cargo.toml -> <dir>
  return $ dir ++ "/target/release"

getDynamicLibExtension :: LocalBuildInfo -> String
getDynamicLibExtension lbi =
  let Platform _ os = hostPlatform lbi
   in case os of
        OSX -> "dylib"
        Linux -> "so"
        Windows -> "dll"
        _ -> error "Unsupported OS"