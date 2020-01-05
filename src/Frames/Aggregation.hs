{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Frames.Aggregation where

import qualified Control.MapReduce             as MR
import           Control.MapReduce                 -- for re-export
import qualified Frames.MapReduce              as FMR

import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.Hashable                 as Hash
import qualified Data.List                     as L
import           Data.Maybe                     ( isJust )
import           Data.Monoid                    ( Monoid(..) )
import           Data.Hashable                  ( Hashable )
import           Data.Kind                      ( Type )
import           GHC.TypeLits                   ( Symbol )

import qualified Frames                        as F
import           Frames                         ( (:.) )
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import           Data.Vinyl                     ( ElField )
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.SRec               as V
import qualified Data.Vinyl.ARec               as V
import qualified Foreign.Storable              as FS

-- | Given some group keys in columns k,
-- some keys to aggregate over in columns ak,
-- some keys to aggregate into in (new) columns ak',
-- a (hopefully surjective) map from records of ak to records of ak',
-- and a fold over the data, in columns d, aggregating over the rows
-- where ak was distinct but ak' is not,
-- produce a fold to transform data keyed by k and ak to data keyed
-- by k and ak' with appropriate aggregations done in the d.
-- E.g., suppose you have voter turnout data for all 50 states in the US,
-- keyed by state and age of voter in years.  The data is two columns:
-- total votes cast and turnout as a percentage.
-- You want to aggregate the ages into two bands, over and under some age.
-- So your k is the state column, ak is the age column, ak' is a new column with
-- data type to indicate over/under.  The Fold has to sum over the total votes and
-- perform a weighted-sum over the percentages.
aggregateFold
  :: forall ak ak' d
   . ( F.Disjoint ak d ~ True
     , F.Disjoint ak' d ~ True
     , (ak' V.++ d) F.⊆ ((ak V.++ d) V.++ ak')
     , ak F.⊆ (ak V.++ d)
     , ak' F.⊆ (ak' V.++ d)
     , d F.⊆ (ak' V.++ d)
     , Ord (F.Record ak')
     , FI.RecVec (ak' V.++ d)
     , Ord (F.Record ak)
     )
  => (F.Record ak -> F.Record ak') -- ^ get aggregated key from key
  -> (FL.Fold (F.Record d) (F.Record d)) -- ^ aggregate data
  -> FL.Fold (F.Record (ak V.++ d)) (F.FrameRec (ak' V.++ d))
aggregateFold toAggKey aggDataF =
  let aggUnpack =
        MR.Unpack
          (\r -> [F.rcast @(ak' V.++ d) $ r `V.rappend` (toAggKey (F.rcast r))]) -- add new keys, lose old
      aggAssign = FMR.assignKeysAndData @ak' @d
  in  FMR.concatFold
        $ FMR.mapReduceFold aggUnpack aggAssign (FMR.foldAndAddKey aggDataF)


mergeDataFolds
  :: FL.Fold (F.Record d) (F.Record '[a])
  -> FL.Fold (F.Record d) (F.Record '[b])
  -> FL.Fold (F.Record d) (F.Record '[a, b])
mergeDataFolds aF bF = V.rappend <$> aF <*> bF
