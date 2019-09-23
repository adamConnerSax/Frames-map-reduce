{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE InstanceSigs          #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Frames.Monomorphic.MapReduce
Description : Helpers for using the map-reduce-folds package with Frames.  Monomorphic in record and interpretation functor.
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Frames-map-reduce provides helper functions for using <https://hackage.haskell.org/package/map-reduce-folds-0.1.0.0 map-reduce-folds>
with <http://hackage.haskell.org/package/Frames Frames>.  Please see those packages for more details.
-}
module Frames.MapReduce
  (
    -- * Unpackers
    unpackFilterRow
  , unpackFilterOnField
  , unpackGoodRows

    -- * Assigners
  , assignKeysAndData
  , assignKeys
  , splitOnKeys

  -- * Reduce and Re-Attach Key Cols
  , reduceAndAddKey
  , foldAndAddKey

  -- * Re-Attach Key Cols
  , makeRecsWithKey
  , makeRecsWithKeyM

  -- * Re-Exports
  , module Control.MapReduce
  )
where

import qualified Control.MapReduce             as MR
import           Control.MapReduce                 -- for re-export

import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.Hashable                 as Hash
import qualified Data.List                     as L
import           Data.Monoid                    ( Monoid(..) )
import           Data.Hashable                  ( Hashable )

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import           Data.Coerce                    ( coerce )

-- | This is only here so we can use hash maps for the grouping step.  This should properly be in Frames itself.
instance Hash.Hashable (F.Record '[]) where
  hash = const 0
  {-# INLINABLE hash #-}
  hashWithSalt s = const s -- TODO: this seems BAD! Or not?
  {-# INLINABLE hashWithSalt #-}

instance (V.KnownField t, Hash.Hashable (V.Snd t), Hash.Hashable (F.Record rs), rs F.⊆ (t ': rs)) => Hash.Hashable (F.Record (t ': rs)) where
  hashWithSalt s r = s `Hash.hashWithSalt` (F.rgetField @t r) `Hash.hashWithSalt` (F.rcast @rs r)
  {-# INLINABLE hashWithSalt #-}

-- | Filter records using a function on the entire record. 
unpackFilterRow
  :: (F.Record rs -> Bool) -> MR.Unpack (F.Record rs) (F.Record rs)
unpackFilterRow test = MR.Filter test

-- | Filter records based on a condition on only one field in the row.  Will usually require a Type Application to indicate which field.
unpackFilterOnField
  :: forall t rs
   . (V.KnownField t, F.ElemOf rs t)
  => (V.Snd t -> Bool)
  -> MR.Unpack (F.Record rs) (F.Record rs)
unpackFilterOnField test = unpackFilterRow (test . F.rgetField @t)

-- | An unpack step which specifies a subset of columns, cs, (via a type-application) and then filters a @Rec (Maybe :. Elfield) rs@
-- to only rows which have all good data in that subset.
unpackGoodRows
  :: forall cs rs
   . (cs F.⊆ rs)
  => MR.Unpack (F.Rec (Maybe F.:. F.ElField) rs) (F.Record cs)
unpackGoodRows = MR.Unpack $ F.recMaybe . F.rcast

-- | Assign both keys and data cols.  Uses type applications to specify them if they cannot be inferred.
-- Keys usually can't. Data sometimes can.
assignKeysAndData
  :: forall ks cs rs
   . (ks F.⊆ rs, cs F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
assignKeysAndData = MR.assign (F.rcast @ks) (F.rcast @cs)
{-# INLINABLE assignKeysAndData #-}

-- | Assign keys and leave all columns, including the keys, in the data passed to reduce.
assignKeys
  :: forall ks rs
   . (ks F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record rs)
assignKeys = MR.assign (F.rcast @ks) id
{-# INLINABLE assignKeys #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to reduce.
splitOnKeys
  :: forall ks rs cs
   . (ks F.⊆ rs, cs ~ F.RDeleteAll ks rs, cs F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

-- | Reduce the data to a single row and then re-attach the key.
reduceAndAddKey
  :: forall ks cs x
   . FI.RecVec ((ks V.++ cs))
  => (forall h . Foldable h => h x -> F.Record cs) -- ^ reduction step
  -> MR.Reduce (F.Record ks) x (F.FrameRec (ks V.++ cs))
reduceAndAddKey process =
  fmap (F.toFrame . pure @[]) $ MR.processAndLabel process V.rappend
{-# INLINABLE reduceAndAddKey #-}

-- | Reduce by folding the data to a single row and then re-attaching the key.
foldAndAddKey
  :: (FI.RecVec ((ks V.++ cs)))
  => FL.Fold x (F.Record cs) -- ^ reduction fold
  -> MR.Reduce (F.Record ks) x (F.FrameRec (ks V.++ cs))
foldAndAddKey fld = fmap (F.toFrame . pure @[]) $ MR.foldAndLabel fld V.rappend  -- is Frame a reasonably fast thing for many appends?
{-# INLINABLE foldAndAddKey #-}

-- | Transform a reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKey
  :: (Functor g, Foldable g, (FI.RecVec (ks V.++ as)))
  => (y -> F.Record as) -- ^ map a result to a record
  -> MR.Reduce (F.Record ks) x (g y) -- ^ original reduce
  -> MR.Reduce (F.Record ks) x (F.FrameRec (ks V.++ as))
makeRecsWithKey makeRec reduceToY = fmap F.toFrame
  $ MR.reduceMapWithKey addKey reduceToY
  where addKey k = fmap (V.rappend k . makeRec)
{-# INLINABLE makeRecsWithKey #-}

-- | Transform an effectful reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKeyM
  :: (Monad m, Functor g, Foldable g, (FI.RecVec (ks V.++ as)))
  => (y -> F.Record as) -- ^ map a result to a record
  -> MR.ReduceM m (F.Record ks) x (g y) -- ^ original reduce
  -> MR.ReduceM m (F.Record ks) x (F.FrameRec (ks V.++ as))
makeRecsWithKeyM makeRec reduceToY = fmap F.toFrame
  $ MR.reduceMMapWithKey addKey reduceToY
  where addKey k = fmap (V.rappend k . makeRec)
{-# INLINABLE makeRecsWithKeyM #-}
