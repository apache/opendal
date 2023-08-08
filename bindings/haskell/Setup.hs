import Distribution.Simple
import Distribution.Simple.Utils
import Distribution.Simple.Setup
import qualified Distribution.PackageDescription as PD
import Distribution.Simple.LocalBuildInfo
import Data.Maybe
import System.Directory
import System.Process

main :: IO ()
main = defaultMainWithHooks simpleUserHooks
    { confHook = rustConfHook,
      buildHook = rustBuildHook
    }

rustConfHook :: (PD.GenericPackageDescription, PD.HookedBuildInfo) -> ConfigFlags -> IO LocalBuildInfo
rustConfHook (description, buildInfo) flags = do
  localBuildInfo <- confHook simpleUserHooks (description, buildInfo) flags
  let packageDescription = localPkgDescr localBuildInfo
      library = fromJust $ PD.library packageDescription
      libraryBuildInfo = PD.libBuildInfo library
  cargoPath <- readProcess "cargo" ["locate-project","--workspace","--message-format=plain"] ""
  let dir = take (length cargoPath - 11) cargoPath -- <dir>/Cargo.toml -> <dir>
  return localBuildInfo
    { localPkgDescr = packageDescription
      { PD.library = Just $ library
        { PD.libBuildInfo = libraryBuildInfo
          { PD.extraLibDirs = (dir ++ "/target/release") :
            PD.extraLibDirs libraryBuildInfo
    } } } }

rustBuildHook :: PD.PackageDescription -> LocalBuildInfo -> UserHooks -> BuildFlags -> IO ()
rustBuildHook pkg_descr lbi hooks flags = do
  putStrLn "Building Rust code..."
  rawSystemExit (fromFlag $ buildVerbosity flags) "cargo" ["build","--release"]
  putStrLn "Build Rust code success!"
  buildHook simpleUserHooks pkg_descr lbi hooks flags