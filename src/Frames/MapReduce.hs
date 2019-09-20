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
Module      : Frames.MapReduce
Description : Helpers for using the map-reduce-folds package with Frames
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
  , unpackGoodRowsToRecord

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

  -- * coercion helpers for (Rec (Identity :. ElField) rs <-> Rec ElField rs)
  , coerceToRecord
  , coerceFromRecord

  -- * Re-Exports
  , module Control.MapReduce
  )
where

import qualified Control.MapReduce             as MR
import           Control.MapReduce                 -- for re-export
import qualified Frames.MapReduce.General      as MG
import qualified Frames.MapReduce.Maybe        as MM
                                                ( unpackGoodRecRows )
import           Control.Arrow                  ( first )
import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.Hashable                 as Hash
import qualified Data.List                     as L
import           Data.Monoid                    ( Monoid(..) )
import           Data.Hashable                  ( Hashable(..) )
import           Data.Maybe                     ( fromJust )
import qualified Data.Profunctor               as P

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import           Data.Coerce                    ( coerce )

-- | coercion from the generalized form
coerceToRecord
  :: V.RMap rs => V.Rec (V.Identity V.:. V.ElField) rs -> F.Record rs
coerceToRecord = V.rmap coerce

-- | coercion to the generalized form
coerceFromRecord
  :: V.RMap rs => F.Record rs -> V.Rec (V.Identity V.:. V.ElField) rs
coerceFromRecord = V.rmap coerce

coercePF
  :: (P.Profunctor p, V.RMap as, V.RMap bs)
  => p
       (V.Rec (V.Identity V.:. V.ElField) as)
       (V.Rec (V.Identity V.:. V.ElField) bs)
  -> p (F.Record as) (F.Record bs)
coercePF = P.dimap coerceFromRecord coerceToRecord

-- | This is only here so we can use hash maps for the grouping step.  This should properly be in Frames itself.
instance (V.RMap rs, Hashable (V.Rec (V.Identity V.:. V.ElField) rs)) => Hashable (F.Record rs) where
  hash = hash . coerceFromRecord -- const 0
  {-# INLINABLE hash #-}
  hashWithSalt s = hashWithSalt s . coerceFromRecord
  {-# INLINABLE hashWithSalt #-}

-- | Filter records using a function on the entire record. 
unpackFilterRow
  :: V.RMap rs => (F.Record rs -> Bool) -> MR.Unpack (F.Record rs) (F.Record rs)
unpackFilterRow test = coercePF $ MG.unpackFilterRow (test . coerceToRecord)

-- | Filter records based on a condition on only one field in the row.  Will usually require a Type Application to indicate which field.
unpackFilterOnField
  :: forall t rs
   . (V.KnownField t, F.ElemOf rs t, V.RMap rs)
  => (V.Snd t -> Bool)
  -> MR.Unpack (F.Record rs) (F.Record rs)
unpackFilterOnField test =
  coercePF $ MG.unpackFilterOnField @t (test . V.getIdentity) --unpackFilterRow (test . F.rgetField @t)

-- | An unpack step which specifies a subset of columns, cs, (via a type-application) and then filters a @Rec (Maybe :. Elfield) rs@
-- to only rows which have all good data in that subset.
unpackGoodRecRows
  :: forall cs rs
   . (cs F.⊆ rs)
  => MR.Unpack
       (F.Rec (Maybe F.:. F.ElField) rs)
       (F.Rec (Maybe F.:. F.ElField) rs)
unpackGoodRecRows = MM.unpackGoodRecRows @cs

-- | Used to be "unpackGoodRows". This name change is breaking.
unpackGoodRowsToRecord
  :: forall cs rs
   . (cs F.⊆ rs, V.RMap cs)
  => MR.Unpack (F.Rec (Maybe F.:. F.ElField) rs) (F.Record cs)
unpackGoodRowsToRecord =
  fmap (V.rmap (fromJust . V.getCompose) . F.rcast @cs) $ unpackGoodRecRows @cs

mapAssignKeys :: (k -> k') -> MR.Assign k y c -> MR.Assign k' y c
mapAssignKeys f (Assign g) = Assign g' where g' y = first f $ g y

coerceAssignKey
  :: (V.RMap rs)
  => MR.Assign (F.Rec (V.Identity F.:. F.ElField) rs) a b
  -> MR.Assign (F.Record rs) a b
coerceAssignKey = mapAssignKeys coerceToRecord

coerceAssign
  :: (V.RMap ks, V.RMap rs, V.RMap cs)
  => Assign
       (F.Rec (V.Identity F.:. F.ElField) ks)
       (F.Rec (V.Identity F.:. F.ElField) rs)
       (F.Rec (V.Identity F.:. F.ElField) cs)
  -> Assign (F.Record ks) (F.Record rs) (F.Record cs)
coerceAssign = coerceAssignKey . coercePF


-- | Assign both keys and data cols.  Uses type applications to specify them if they cannot be inferred.
-- Keys usually can't. Data sometimes can.
assignKeysAndData
  :: forall ks cs rs
   . (ks F.⊆ rs, cs F.⊆ rs, V.RMap ks, V.RMap rs, V.RMap cs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
assignKeysAndData = coerceAssign $ MG.assignKeysAndData --MR.assign (F.rcast @ks) (F.rcast @cs)
{-# INLINABLE assignKeysAndData #-}

-- | Assign keys and leave all columns, including the keys, in the data passed to reduce.
assignKeys
  :: forall ks rs
   . (ks F.⊆ rs, V.RMap rs, V.RMap ks)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record rs)
assignKeys = coerceAssign $ MG.assignKeys  --MR.assign (F.rcast @ks) id
{-# INLINABLE assignKeys #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to reduce.
splitOnKeys
  :: forall ks rs cs
   . ( ks F.⊆ rs
     , cs ~ F.RDeleteAll ks rs
     , cs F.⊆ rs
     , V.RMap ks
     , V.RMap rs
     , V.RMap cs
     )
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

contramapReduceKey :: (k -> k') -> MR.Reduce k' y c -> MR.Reduce k y c
contramapReduceKey f (Reduce g) = Reduce (g . f)

coerceReduceKey
  :: (V.RMap rs)
  => MR.Reduce (F.Rec (V.Identity F.:. F.ElField) rs) a b
  -> MR.Reduce (F.Record rs) a b
coerceReduceKey = contramapReduceKey coerceFromRecord

coerceReduce
  :: (V.RMap ks, V.RMap cs)
  => MR.Reduce
       (F.Rec (V.Identity F.:. F.ElField) ks)
       x
       (F.Rec (V.Identity F.:. F.ElField) cs)
  -> MR.Reduce (F.Record ks) x (F.Record cs)
coerceReduce = coerceReduceKey . fmap coerceToRecord


-- | Reduce the data to a single row and then re-attach the key.
reduceAndAddKey
  :: forall ks cs x
   . (V.RMap ks, V.RMap (ks V.++ cs), V.RMap cs, FI.RecVec ((ks V.++ cs)))
  => (forall h . Foldable h => h x -> F.Record cs) -- ^ reduction step
  -> MR.Reduce (F.Record ks) x (F.FrameRec (ks V.++ cs))
reduceAndAddKey process =
  fmap (F.toFrame . pure @[]) $ coerceReduce $ MG.reduceAndAddKey
    (coerceFromRecord . process)
--  fmap (F.toFrame . pure @[]) $ MR.processAndLabel process V.rappend
{-# INLINABLE reduceAndAddKey #-}

-- | Reduce by folding the data to a single row and then re-attaching the key.
foldAndAddKey
  :: (V.RMap ks, V.RMap (ks V.++ cs), V.RMap cs, FI.RecVec ((ks V.++ cs)))
  => FL.Fold x (F.Record cs) -- ^ reduction fold
  -> MR.Reduce (F.Record ks) x (F.FrameRec (ks V.++ cs))
foldAndAddKey fld =
  fmap (F.toFrame . pure @[]) $ coerceReduce $ MG.foldAndAddKey
    (fmap coerceFromRecord fld) --MR.foldAndLabel fld V.rappend  -- is Frame a reasonably fast thing for many appends?
{-# INLINABLE foldAndAddKey #-}

-- | Transform a reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKey
  :: ( Functor g
     , Foldable g
     , V.RMap ks
     , V.RMap (ks V.++ as)
     , V.RMap as
     , (FI.RecVec (ks V.++ as))
     )
  => (y -> F.Record as) -- ^ map a result to a record
  -> MR.Reduce (F.Record ks) x (g y) -- ^ original reduce
  -> MR.Reduce (F.Record ks) x (F.FrameRec (ks V.++ as))
makeRecsWithKey makeRec reduceToY = fmap F.toFrame $ _ $ MG.makeRecsWithKey
  (coerceFromRecord . makeRec)
  (contramapReduceKey coerceToRecord reduceToY)
{-
fmap F.toFrame
  $ MR.reduceMapWithKey addKey reduceToY
  where addKey k = fmap (V.rappend k . makeRec)
{-# INLINABLE makeRecsWithKey #-}
-}

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
