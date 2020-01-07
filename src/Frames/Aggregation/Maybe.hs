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
Module      : Frames.Aggregation.Maybe
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
module Frames.Aggregation.Maybe
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
                                                )

import qualified Frames.Aggregation.General    as FA
import           Frames.Aggregation.General     ( RecordKeyMap )
import qualified Frames                        as F
import           Frames                         ( (:.) )
import qualified Frames.Melt                   as F
import qualified Data.Vinyl                    as V
import           Data.Vinyl                     ( ElField )
import qualified Data.Vinyl.TypeLevel          as V
import qualified Control.Foldl                 as FL

--import qualified Control.MapReduce             as MR
--import qualified Frames.MapReduce.General      as FMR




import           GHC.TypeLits                   ( Symbol )
import           Data.Kind                      ( Type )


-- | Combine 2 key aggregation functions over disjoint columns.
combineKeyAggregations
  :: forall (a :: [(Symbol, Type)]) b a' b' record
   . ( a F.⊆ (a V.++ b)
     , b F.⊆ (a V.++ b)
     , F.Disjoint a' b' ~ 'True
     , RCastC a (a V.++ b) record Maybe
     , RCastC b (a V.++ b) record Maybe
     , IsoRec a' record Maybe
     , IsoRec b' record Maybe
     , IsoRec (a' V.++ b') record Maybe
     )
  => RecordKeyMap record Maybe a a'
  -> RecordKeyMap record Maybe b b'
  -> RecordKeyMap record Maybe (a V.++ b) (a' V.++ b')
combineKeyAggregations = FA.combineKeyAggregations

-- | Promote an ordinary function @a -> b@ to a @RecordKeyMap aCol bCol@ where
-- @aCol@ holds values of type @a@ and @bCol@ holds values of type @b@.
keyMap
  :: forall a b record
   . ( V.KnownField a
     , V.KnownField b
     , RecGetFieldC a record Maybe '[a]
     , IsoRec '[b] record Maybe
     , Applicative Maybe
     )
  => (V.Snd a -> V.Snd b)
  -> RecordKeyMap record Maybe '[a] '[b]
keyMap = FA.keyMap

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
  :: forall (ak :: [(Symbol, Type)]) ak' d record
   . ( (ak' V.++ d) F.⊆ ((ak V.++ d) V.++ ak')
     , ak F.⊆ (ak V.++ d)
     , ak' F.⊆ (ak' V.++ d)
     , d F.⊆ (ak' V.++ d)
     , Ord (record (Maybe :. ElField) ak')
     , Ord (record (Maybe :. ElField) ak)
     , RCastC (ak' V.++ d) ((ak V.++ d) V.++ ak') record Maybe
     , RCastC ak (ak V.++ d) record Maybe
     , RCastC ak' (ak' V.++ d) record Maybe
     , RCastC d (ak' V.++ d) record Maybe
     , IsoRec d record Maybe
     , IsoRec (ak V.++ d) record Maybe
     , IsoRec (ak' V.++ d) record Maybe
     , IsoRec ak' record Maybe
     , IsoRec ((ak V.++ d) V.++ ak') record Maybe
     )
  => RecordKeyMap record Maybe ak ak' -- ^ get aggregated key from key
  -> (FL.Fold (record (Maybe :. ElField) d) (record (Maybe :. ElField) d)) -- ^ aggregate data
  -> FL.Fold
       (record (Maybe :. ElField) (ak V.++ d))
       [(record (Maybe :. ElField) (ak' V.++ d))]
aggregateAllFold = FA.aggregateAllFold

-- | Aggregate key columns @ak@ into @ak'@ while leaving key columns @k@ along.
-- Allows aggregation over only some fields.  Will often require a typeapplication
-- to specify what @k@ is.
aggregateFold
  :: forall (k :: [(Symbol, Type)]) ak ak' d record
   . ( (ak' V.++ d) F.⊆ ((ak V.++ d) V.++ ak')
     , ak F.⊆ (ak V.++ d)
     , ak' F.⊆ (ak' V.++ d)
     , d F.⊆ (ak' V.++ d)
     , Ord (record (Maybe :. ElField) ak')
     , Ord (record (Maybe :. ElField) ak)
     , (k V.++ (ak' V.++ d)) ~ ((k V.++ ak') V.++ d)
     , Ord (record (Maybe :. ElField) k)
     , k F.⊆ ((k V.++ ak') V.++ d)
     , k F.⊆ ((k V.++ ak) V.++ d)
     , (ak V.++ d) F.⊆ ((k V.++ ak) V.++ d)
     , RCastC ak (ak V.++ d) record Maybe
     , RCastC ak' (ak' V.++ d) record Maybe
     , RCastC d (ak' V.++ d) record Maybe
     , RCastC k ((k V.++ ak) V.++ d) record Maybe
     , RCastC (ak V.++ d) ((k V.++ ak) V.++ d) record Maybe
     , RCastC (ak' V.++ d) ((ak V.++ d) V.++ ak') record Maybe
     , IsoRec k record Maybe
     , IsoRec d record Maybe
     , IsoRec ((k V.++ ak') V.++ d) record Maybe
     , IsoRec (ak V.++ d) record Maybe
     , IsoRec (ak' V.++ d) record Maybe
     , IsoRec ak' record Maybe
     , IsoRec ((ak V.++ d) V.++ ak') record Maybe
     )
  => RecordKeyMap record Maybe ak ak' -- ^ get aggregated key from key
  -> (FL.Fold (record (Maybe :. ElField) d) (record (Maybe :. ElField) d)) -- ^ aggregate data
  -> FL.Fold
       (record (Maybe :. ElField) (k V.++ ak V.++ d))
       [record (Maybe :. ElField) (k V.++ ak' V.++ d)]
aggregateFold = FA.aggregateFold @k

mergeDataFolds
  :: forall (a :: (Symbol, Type)) b d record
   . ( IsoRec '[b] record Maybe
     , IsoRec '[a] record Maybe
     , IsoRec '[a, b] record Maybe
     )
  => FL.Fold (record (Maybe :. ElField) d) (record (Maybe :. ElField) '[a])
  -> FL.Fold
       (record (Maybe :. ElField) d)
       (record (Maybe :. ElField) '[b])
  -> FL.Fold
       (record (Maybe :. ElField) d)
       (record (Maybe :. ElField) '[a, b])
mergeDataFolds = FA.mergeDataFolds
