# Frames-map-reduce- v 0.1.0.0

[![Build Status][travis-badge]][travis]
[![Hackage][hackage-badge]][hackage]
[![Hackage Dependencies][hackage-deps-badge]][hackage-deps]

This library contains some useful functions for using the [map-reduce-folds](https://hackage.haskell.org/package/map-reduce-folds-0.1.0.0) package with Frames from the [Frames](http://hackage.haskell.org/package/Frames).  Included, in Frames.MapReduce, are helpers for filtering Frames, splitting records into key and data columns and reattaching key columns after reducing.

Also included, in the Frames.Folds module, are some helpful functions for building folds of Frames from folds over each column, specified either individually or via a constraint on all the columns being folded over.

For example:

```haskell
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
      randDbls   = L.take (2 * n) $ randomRs (0.0, 100.0) g
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
```


_______


LICENSE (BSD-3-Clause)
_______
Copyright (c) 2018, Adam Conner-Sax, All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
      copyright notice, this list of conditions and the following
      disclaimer in the documentation and/or other materials provided
      with the distribution.

    * Neither the name of Adam Conner-Sax nor the names of other
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


[travis]:        <https://travis-ci.org/adamConnerSax/Frames-map-reduce>
[travis-badge]:  <https://travis-ci.org/adamConnerSax/Frames-map-reduce.svg?branch=master>
[hackage]:       <https://hackage.haskell.org/package/Frames-map-reduce>
[hackage-badge]: <https://img.shields.io/hackage/v/Frames-map-reduce.svg>
[hackage-deps-badge]: <https://img.shields.io/hackage-deps/v/Frames-map-reduce.svg>
[hackage-deps]: <http://packdeps.haskellers.com/feed?needle=Frames-map-reduce>
