import Test.Tasty
import Test.Tasty.Ingredients.Basic (consoleTestReporter)

import BasicTest

main :: IO ()
main = do
  tests <-
    testGroup "All Tests"
      <$> sequence
        [ return basicTests
        -- Add other test groups here as needed
        ]
  defaultMainWithIngredients [consoleTestReporter] tests