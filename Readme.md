# Frames-map-reduce- v 0.3.0.0

[![Build Status][travis-badge]][travis]
[![Hackage][hackage-badge]][hackage]
[![Hackage Dependencies][hackage-deps-badge]][hackage-deps]

This library contains some useful functions for using the 
[map-reduce-folds](https://hackage.haskell.org/package/map-reduce-folds-0.1.0.0) 
package with Frames (containers of data rows) from the 
[Frames](http://hackage.haskell.org/package/Frames) package.
Included, in Frames.MapReduce, are helpers for filtering Frames,
splitting records into key and data columns and reattaching key columns after reducing.

There is also support for using the map-reduce-folds library with 
Vinyl records of more complex type, e.g. ```ARec (Maybe :. ElField) rs```.
The specific case of ```Maybe``` is handled in Frames.MapReduce.Maybe and 
the fully polymorphic case (supporting ```Rec```, ```ARec``` and ```SRec``` 
and any interpretation functor composed with ElField which has a reasonable 
interpretation as ```Maybe```, e.g., ```Either```) is handled in 
Frames.MapReduce.General.

Also included, in the Frames.Folds (and Frames.Folds.Maybe, Frames.Folds.General) module, 
are some helpful functions for building folds of Frames from folds over each column, 
specified either individually or via a constraint on all the columns being folded over.

There is a helper for a common map-reduce, namely an aggregation where keys of one type
are merged to some smaller set of keys and some data columns are aggregated on the
merged group.  Given a function from old keys to new keys (as records) and a fold over
the data expressing the aggregation, this function will build the fold to do the 
aggregation.  And there is a helper function for building the data fold from folds
for each field.

NB: The functions which operate on ```Record rs```, ```record (Maybe :. ElField) rs``` 
and ```Applicative f => record (f :. ElField)```, have the same names but reside in 
different modules.  I assume most users will want only one version.  But if, as in the 
example provided, you want to use two versions in the same module you will need to import
at least one of them qualified.

For example, given a Frame with three columns, a text column ```Label``` and two columns, ```X``` and ```Y```, holding doubles, we

* unpack, filtering using ```unpackFilterOnField``` (with a type-application to specify the ```Label``` column), 
* assign, thus grouping by ```Label``` and feeding the rest of the columns to 
reduce using ```splitOnKeys``` with a type-application to specify which columns are the key.

* reduce by folding over the two remaining columns using the ```foldAllConstrained``` 
function. The type-application here specifies a constraint satisfied by all the columns 
being folded, and then the cols to fold.  
This last part is a little complex.  See the Frames.Folds modules for more details.

We can also demonstrate the functions operating on ```Rec (Maybe :. ElField)``` rows.

* unpack, doing nothing.
* assign as above, using the ```Maybe``` generalized version of ```splitOnKeys```.
* reduce as above, using the ```Maybe``` generalized versions of ```foldAndAddKey``` and ```foldAllConstrained```.

```haskell
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE InstanceSigs      #-}
module Main where
import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import qualified Data.Text                     as T
import qualified Data.Vinyl                    as V
import           Data.Vinyl.Functor             ( Compose(..)
                                                , (:.)
                                                )
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.Folds                  as FF
import qualified Frames.Folds.Maybe            as FFM
import qualified Frames.MapReduce              as FMR
import qualified Frames.MapReduce.Maybe        as FMRM
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
       (F.Rec (Maybe :. F.ElField) '[X, Y])
assign' = FMRM.splitOnKeys @'[Label]

reduce'
  :: FMR.Reduce
       (F.Rec (Maybe :. F.ElField) '[Label])
       (F.Rec (Maybe :. F.ElField) '[X, Y])
       (F.Rec (Maybe :. F.ElField) '[Label, X, Y])
reduce' = FMRM.foldAndAddKey $ (FFM.foldAllConstrained @Num @'[X, Y]) FL.sum


mrFold'
  :: FMR.Fold
       (F.Rec (Maybe :. F.ElField) '[Label, X, Y])
       [F.Rec (Maybe :. F.ElField) '[Label, X, Y]]
mrFold' = FMR.mapReduceFold unpack' assign' reduce'


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
