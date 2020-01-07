{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
       {-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Frames.Aggregation.General
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
module Frames.Aggregation.General
  (
  -- * Type-alias for maps from one record key to another
    RecordKeyMap
    -- * Aggregation Function combinators
  , combineKeyAggregations
  , keyMap
    -- * aggregationFolds
  , aggregateAllFold
  , aggregateFold
  , mergeDataFolds
  )
where

import           Frames.MapReduce.General       ( RecGetFieldC(..)
                                                , RCastC(..)
                                                , IsoRec(..)
                                                , isoRecAppend
                                                )

import qualified Control.MapReduce             as MR
import qualified Frames.MapReduce.General      as FMR

import qualified Control.Foldl                 as FL

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

import           Data.Vinyl                     ( ElField )
import qualified Data.Vinyl.Functor            as V
import           Frames                         ( (:.) )

import           GHC.TypeLits                   ( Symbol )
import           Data.Kind                      ( Type )

-- | Type-alias for key aggregation functions.
type RecordKeyMap record f k k' = record (f :. ElField) k -> record (f :. ElField) k'

-- | Combine 2 key aggregation functions over disjoint columns.
combineKeyAggregations
  :: forall (a :: [(Symbol, Type)]) b a' b' record f
   . ( a F.⊆ (a V.++ b)
     , b F.⊆ (a V.++ b)
     , F.Disjoint a' b' ~ 'True
     , RCastC a (a V.++ b) record f
     , RCastC b (a V.++ b) record f
     , IsoRec a' record f
     , IsoRec b' record f
     , IsoRec (a' V.++ b') record f
     )
  => RecordKeyMap record f a a'
  -> RecordKeyMap record f b b'
  -> RecordKeyMap record f (a V.++ b) (a' V.++ b')
combineKeyAggregations aToa' bTob' r =
  aToa' (rcastF r) `isoRecAppend` bTob' (rcastF r)

-- | Promote an ordinary function @a -> b@ to a @RecordKeyMap aCol bCol@ where
-- @aCol@ holds values of type @a@ and @bCol@ holds values of type @b@.
keyMap
  :: forall a b record f
   . ( V.KnownField a
     , V.KnownField b
     , RecGetFieldC a record f '[a]
     , IsoRec '[b] record f
     , Applicative f
     )
  => (V.Snd a -> V.Snd b)
  -> RecordKeyMap record f '[a] '[b]
keyMap g r =
  fromRec
    $ (V.Compose . fmap (V.Field . g . V.getField) . V.getCompose) (rgetF @a r)
    V.:& V.RNil

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
  :: forall (ak :: [(Symbol, Type)]) ak' d record f
   . ( (ak' V.++ d) F.⊆ ((ak V.++ d) V.++ ak')
     , ak F.⊆ (ak V.++ d)
     , ak' F.⊆ (ak' V.++ d)
     , d F.⊆ (ak' V.++ d)
     , Ord (record (f :. ElField) ak')
     , Ord (record (f :. ElField) ak)
     , RCastC (ak' V.++ d) ((ak V.++ d) V.++ ak') record f
     , RCastC ak (ak V.++ d) record f
     , RCastC ak' (ak' V.++ d) record f
     , RCastC d (ak' V.++ d) record f
     , IsoRec d record f
     , IsoRec (ak V.++ d) record f
     , IsoRec (ak' V.++ d) record f
     , IsoRec ak' record f
     , IsoRec ((ak V.++ d) V.++ ak') record f
     )
  => RecordKeyMap record f ak ak' -- ^ get aggregated key from key
  -> (FL.Fold (record (f :. ElField) d) (record (f :. ElField) d)) -- ^ aggregate data
  -> FL.Fold
       (record (f :. ElField) (ak V.++ d))
       [(record (f :. ElField) (ak' V.++ d))]
aggregateAllFold toAggKey aggDataF =
  let aggUnpack
        :: MR.Unpack
             (record (f :. ElField) (ak V.++ d))
             (record (f :. ElField) (ak' V.++ d))
      aggUnpack =
        MR.Unpack (\r -> [rcastF $ r `isoRecAppend` toAggKey (rcastF r)]) -- add new keys, lose old
      aggAssign = FMR.assignKeysAndData @ak' @d
  in  MR.mapReduceFold aggUnpack aggAssign (FMR.foldAndAddKey aggDataF)

-- | Aggregate key columns @ak@ into @ak'@ while leaving key columns @k@ along.
-- Allows aggregation over only some fields.  Will often require a typeapplication
-- to specify what @k@ is.
aggregateFold
  :: forall (k :: [(Symbol, Type)]) ak ak' d record f
   . ( (ak' V.++ d) F.⊆ ((ak V.++ d) V.++ ak')
     , ak F.⊆ (ak V.++ d)
     , ak' F.⊆ (ak' V.++ d)
     , d F.⊆ (ak' V.++ d)
     , Ord (record (f :. ElField) ak')
     , Ord (record (f :. ElField) ak)
     , (k V.++ (ak' V.++ d)) ~ ((k V.++ ak') V.++ d)
     , Ord (record (f :. ElField) k)
     , k F.⊆ ((k V.++ ak') V.++ d)
     , k F.⊆ ((k V.++ ak) V.++ d)
     , (ak V.++ d) F.⊆ ((k V.++ ak) V.++ d)
     , RCastC ak (ak V.++ d) record f
     , RCastC ak' (ak' V.++ d) record f
     , RCastC d (ak' V.++ d) record f
     , RCastC k ((k V.++ ak) V.++ d) record f
     , RCastC (ak V.++ d) ((k V.++ ak) V.++ d) record f
     , RCastC (ak' V.++ d) ((ak V.++ d) V.++ ak') record f
     , IsoRec k record f
     , IsoRec d record f
     , IsoRec ((k V.++ ak') V.++ d) record f
     , IsoRec (ak V.++ d) record f
     , IsoRec (ak' V.++ d) record f
     , IsoRec ak' record f
     , IsoRec ((ak V.++ d) V.++ ak') record f
     )
  => RecordKeyMap record f ak ak' -- ^ get aggregated key from key
  -> (FL.Fold (record (f :. ElField) d) (record (f :. ElField) d)) -- ^ aggregate data
  -> FL.Fold
       (record (f :. ElField) (k V.++ ak V.++ d))
       [record (f :. ElField) (k V.++ ak' V.++ d)]
aggregateFold keyAgg aggDataF = MR.concatFold $ MR.mapReduceFold
  MR.noUnpack
  (FMR.assignKeysAndData @k @(ak V.++ d))
  ( FMR.makeRecsWithKey id
  $ MR.ReduceFold (const $ aggregateAllFold keyAgg aggDataF)
  )


mergeDataFolds
  :: forall (a :: (Symbol, Type)) b d record f
   . (IsoRec '[b] record f, IsoRec '[a] record f, IsoRec '[a, b] record f)
  => FL.Fold (record (f :. ElField) d) (record (f :. ElField) '[a])
  -> FL.Fold (record (f :. ElField) d) (record (f :. ElField) '[b])
  -> FL.Fold (record (f :. ElField) d) (record (f :. ElField) '[a, b])
mergeDataFolds aF bF = isoRecAppend <$> aF <*> bF
