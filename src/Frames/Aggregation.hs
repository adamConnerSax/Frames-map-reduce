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
{-|
Module      : Frames.Aggregation
Description : A specialised Map/Reduce for aggregating one set of keys to a smaller one given some operation to merge data.
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Frames.Aggregation.General contains types and functions to support a specific map/reduce operation.  Frequently, data is given
with more specificity than required for downstream operations.  Perhaps an age is given in years and we only need to know the
age-band.  Assuming we know how to aggregagte data columns, we want to perform that aggregation on all the subsets required to
build the data-set with the simpler key, while perhaps leaving some other columns alone.  @aggregateFold@ does this.
-}
module Frames.Aggregation
  (
    -- * Types
    RecordKeyMap
    -- * Constraint Helpers
  , AggregateAllC
  , AggregateC
  , CombineKeyAggregationsC
    -- * Aggregation Function combinators
  , combineKeyAggregations
  , keyMap
    -- * aggregationFolds
  , aggregateAllFold
  , aggregateFold
  , mergeDataFolds
  )
where

import qualified Control.MapReduce             as MR
import qualified Frames.MapReduce              as FMR

import qualified Control.Foldl                 as FL

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

-- | Type-alias for key aggregation functions.
type RecordKeyMap k k' = F.Record k -> F.Record k'

type CombineKeyAggregationsC a a' b b' = (a F.⊆ (a V.++ b), b F.⊆ (a V.++ b), F.Disjoint a' b' ~ 'True)

-- | Combine 2 key aggregation functions over disjoint columns.
combineKeyAggregations
  :: forall a a' b b'.CombineKeyAggregationsC a a' b b'
  => RecordKeyMap a a'
  -> RecordKeyMap b b'
  -> RecordKeyMap (a V.++ b) (a' V.++ b')
combineKeyAggregations aToa' bTob' r =
  aToa' (F.rcast r) `V.rappend` bTob' (F.rcast r)

-- | Promote an ordinary function @a -> b@ to a @RecordKeyMap aCol bCol@ where
-- @aCol@ holds values of type @a@ and @bCol@ holds values of type @b@.
keyMap
  :: forall a b
   . (V.KnownField a, V.KnownField b)
  => (V.Snd a -> V.Snd b)
  -> RecordKeyMap '[a] '[b]
keyMap f r = f (F.rgetField @a r) F.&: V.RNil

type AggregateAllC ak ak' d = ((ak' V.++ d) F.⊆ ((ak V.++ d) V.++ ak')
                              , ak F.⊆ (ak V.++ d)
                              , ak' F.⊆ (ak' V.++ d)
                              , d F.⊆ (ak' V.++ d)
                              , Ord (F.Record ak')
                              , Ord (F.Record ak)
                              , FI.RecVec (ak' V.++ d)
                              )

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
aggregateAllFold
  :: forall ak ak' d
   . AggregateAllC ak ak' d
  => RecordKeyMap ak ak' -- ^ get aggregated key from key
  -> (FL.Fold (F.Record d) (F.Record d)) -- ^ aggregate data
  -> FL.Fold (F.Record (ak V.++ d)) (F.FrameRec (ak' V.++ d))
aggregateAllFold toAggKey aggDataF =
  let aggUnpack =
        MR.Unpack
          (\r -> [F.rcast @(ak' V.++ d) $ r `V.rappend` (toAggKey (F.rcast r))]) -- add new keys, lose old
      aggAssign = FMR.assignKeysAndData @ak' @d
  in  FMR.concatFold
        $ FMR.mapReduceFold aggUnpack aggAssign (FMR.foldAndAddKey aggDataF)

type AggregateC k ak ak' d = (CombineKeyAggregationsC k k ak ak'
                             ,AggregateAllC (k V.++ ak) (k V.++ ak') d
                             )

-- | Aggregate key columns @ak@ into @ak'@ while leaving key columns @k@ alone.
-- Allows aggregation over only some fields.  Will often require a typeapplication
-- to specify what @k@ is.
aggregateFold :: forall k ak ak' d.AggregateC k ak ak' d
  => RecordKeyMap ak ak' -- ^ get aggregated key from key
  -> (FL.Fold (F.Record d) (F.Record d)) -- ^ aggregate data
  -> FL.Fold
       (F.Record (k V.++ ak V.++ d))
       (F.FrameRec (k V.++ ak' V.++ d))
aggregateFold f = aggregateAllFold (combineKeyAggregations @k @k id f)


{-
aggregateFold
  :: forall k ak ak' d
   . ( (ak' V.++ d) F.⊆ ((ak V.++ d) V.++ ak')
     , ak F.⊆ (ak V.++ d)
     , ak' F.⊆ (ak' V.++ d)
     , d F.⊆ (ak' V.++ d)
     , Ord (F.Record ak')
     , FI.RecVec (ak' V.++ d)
     , Ord (F.Record ak)
     , (k V.++ (ak' V.++ d)) ~ ((k V.++ ak') V.++ d)
     , Ord (F.Record k)
     , k F.⊆ ((k V.++ ak') V.++ d)
     , k F.⊆ ((k V.++ ak) V.++ d)
     , (ak V.++ d) F.⊆ ((k V.++ ak) V.++ d)
     , FI.RecVec ((k V.++ ak') V.++ d)
     )
  => RecordKeyMap ak ak' -- ^ get aggregated key from key
  -> (FL.Fold (F.Record d) (F.Record d)) -- ^ aggregate data
  -> FL.Fold
       (F.Record (k V.++ ak V.++ d))
       (F.FrameRec (k V.++ ak' V.++ d))
aggregateFold keyAgg aggDataF = FMR.concatFold $ FMR.mapReduceFold
  MR.noUnpack
  (FMR.assignKeysAndData @k @(ak V.++ d))
  ( FMR.makeRecsWithKey id
  $ MR.ReduceFold (const $ aggregateAllFold keyAgg aggDataF)
  )
-}

mergeDataFolds
  :: FL.Fold (F.Record d) (F.Record '[a])
  -> FL.Fold (F.Record d) (F.Record '[b])
  -> FL.Fold (F.Record d) (F.Record '[a, b])
mergeDataFolds aF bF = V.rappend <$> aF <*> bF
