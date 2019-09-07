{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE InstanceSigs     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts     #-}
module Main where
import           Data.Functor.Classes
import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import           Data.Monoid                    ( Sum )
import qualified Data.Text                     as T
import qualified Data.Vinyl                    as V
import  Data.Vinyl.Functor (Compose(..), (:.))
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.Folds                  as FF
import qualified Frames.Folds.Maybe            as FFM
import qualified Frames.MapReduce              as FMR
import qualified Frames.MapReduce.Maybe        as FMRM
import           Data.Coerce                    ( coerce )
import           System.Random                  ( newStdGen
                                                , randomRs
                                                )

-- Create types for the cols                                                
type Label = "label" F.:-> T.Text
type Y = "y" F.:-> Double
type X = "x" F.:-> Double
type AllCols = '[Label, Y, X]

-- filter, leaving only rows with labels 'A', 'B' or 'C'
unpack = FMR.unpackFilterOnField @Label (`elem` ["A", "B", "C"])

-- group the rest of the cols by Label
assign = FMR.splitOnKeys @'[Label]

-- sum the data columns and then re-attach the key
reduce = FMR.foldAndAddKey $ (FF.foldAllConstrained @Num @'[Y, X]) FL.sum

-- put it all together: filter, group by label, sum the data cols and re-attach the key.
-- Then turn the resulting list of Frames (each with only one Record in this case)
-- into one Frame via (<>).

mrFold = FMR.concatFold $ FMR.mapReduceFold unpack assign reduce


-- Bleh, this should go in Frames.  
instance (Eq (F.ElField a)) => Eq (Compose Maybe F.ElField a) where
  (==) (Compose fga) (Compose fga') = fga == fga'

instance (Ord (F.ElField a)) => Ord (Compose Maybe F.ElField a) where
  compare (Compose fga) (Compose fga') = fga `compare` fga'


unpack'
  :: FMR.Unpack (F.Rec (Maybe :. F.ElField) rs) (F.Rec (Maybe :. F.ElField) rs)
unpack' = FMRM.unpackNoOp

assign'
  :: FMR.Assign
       (F.Rec (Maybe :. F.ElField) '[Label])
       (F.Rec (Maybe :. F.ElField) '[Label, X, Y])
       ( F.Rec (Maybe :. F.ElField) '[X, Y] )
assign' = FMRM.splitOnKeys @'[Label]

-- | Computes the sum of all `Just` elements
maybeSum :: Num a => FL.Fold (Maybe a) (Maybe a)
maybeSum = FL.Fold go Nothing id
 where
  go a b = case a of
    Just a' -> case b of
      Just b' -> Just $ a' + b'
      Nothing -> a
    Nothing -> case b of
      Just _ -> b
      Nothing -> Nothing
{-# INLINABLE maybeSum #-}

reduce'
  :: FMR.Reduce
       (F.Rec (Maybe :. F.ElField) '[Label])
       (F.Rec (Maybe :. F.ElField) '[X, Y])
       (F.Rec (Maybe :. F.ElField) '[Label, X, Y])
reduce' = FMRM.foldAndAddKey $ (FFM.foldAllConstrained @Num @'[X, Y]) maybeSum

-- mrFold' :: FMR.Fold (F.Rec (Maybe :. F.ElField) rs0) [F.Rec (Maybe :. F.ElField) rs0]
mrFold'
  :: FMR.Fold
       (F.Rec (Maybe :. F.ElField) '[Label, X, Y])
       [F.Rec (Maybe :. F.ElField) '[Label, X, Y]]
mrFold' =FMR.mapReduceFold unpack' assign' reduce'

main :: IO ()
main = do
  f <- createFrame 1000
  let result = FMR.fold mrFold f
  putStrLn $ (L.intercalate "\n" $ fmap show $ FL.fold FL.list result)
  let result' = FMR.fold mrFold' createHolyRows
  putStrLn . unlines . fmap show $ FL.fold FL.list result'

{- Output
{label :-> "A", y :-> 1577.3965303339942, x :-> 1507.286289962377}
{label :-> "B", y :-> 1934.223021597267, x :-> 2135.9312483902577}
{label :-> "C", y :-> 1528.6898777108415, x :-> 1810.5096765228654}
{Just label :-> "A", Just x :-> 5.0, Just y :-> 2.0}
{Just label :-> "Z", Just x :-> 5.0, Just y :-> 9.0}
-}

--- create the Frame
createFrame :: Int -> IO (F.FrameRec AllCols)
createFrame n = do
  g <- newStdGen
  let randLabels = L.take n $ randomRs ('A', 'Z') g
      randDbls   = L.take (2 * n) $ randomRs (0.0, 100.0) g
      oneRow m =
        T.singleton (randLabels !! m)
          F.&: (randDbls !! m)
          F.&: (randDbls !! (n + m))
          F.&: V.RNil
  return $ F.toFrame $ fmap oneRow [0 .. (n - 1)]

createHolyRows :: [F.Rec (Maybe F.:. F.ElField) '[Label, X, Y]]
createHolyRows = fmap go [one, two, three, four]
 where
  go =
    V.rmap (either (const (Compose Nothing)) (Compose . Just) . getCompose)
      . F.readRec
  one   = ["A", "1", "2"]
  two   = ["Z", "NaN", "3"]
  three = ["A", "4", "lol"]
  four  = ["Z", "5", "6"]
