{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import qualified Frames.MapReduce              as FMR
import qualified Frames.Folds                  as FF

import qualified Frames                        as F
import qualified Data.Vinyl                    as V
import qualified Data.List                     as L
import qualified Data.Text                     as T
import qualified Control.Foldl                 as FL
import           Data.Monoid                    ( Sum )
import           System.Random                  ( newStdGen
                                                , randomRs
                                                )
type Label = "label" F.:-> T.Text
type Y = "y" F.:-> Double
type X = "x" F.:-> Double
type AllCols = [Label,Y,X]

createFrame :: Int -> IO (F.FrameRec AllCols)
createFrame n = do
  g <- newStdGen
  let randLabels = L.take n $ randomRs ('A', 'Z') g
      randD      = L.take (2 * n) $ randomRs (0.0, 100.0) g
      oneRow m =
        T.singleton (randLabels !! m)
          F.&: (randDbls !! m)
          F.&: (randDbls !! (n + m))
          F.&: V.RNil
  return $ F.toFrame $ fmap oneRow [0 .. (n - 1)]

-- filter, leaving only rows with labels 'A', 'B' or 'C'
unpack = FMR.unpackFilterOnField @Label (`elem` ["A", "B", "C"])

-- group the rest of the cols by Label
assign = FMR.splitOnKeys @'[Label] @AllCols

-- sum the data columns and then re-attach the key
reduce = FMR.foldAndAddKey (FF.foldAllMonoid @Sum @'[Y, X])

mrFold :: FL.Fold (F.Record AllCols) (F.FrameRec AllCols)
mrFold = FMR.concatFold $ FMR.mapReduceFold unpack assign reduce

main :: IO ()
main = do
  f <- createFrame 1000
  let result = FMR.fold mrFold f
  putStrLn $ (L.intercalate "\n" $ fmap show $ FL.fold FL.list result)

{- Output
{label :-> "A", y :-> 1293.6893073755323, x :-> 1386.4314446405742}
{label :-> "B", y :-> 1940.9402110282622, x :-> 2244.645291592506}
{label :-> "C", y :-> 2009.8541388288395, x :-> 2128.7190606123568}
-}
