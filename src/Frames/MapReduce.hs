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

map-reduce-folds provides types and helper functions for expressing folds over a container of things, e.g., rows of data,
in "map-reduce" style.  That is, the initial collection is split (rows are re-shaped or filtered, for example), then assigned a key,
then grouped by that key and then that group is processed and optionally re-joined with the key, leaving you with a new collection.

These operations are folds over the initial data.  But expressing them directly as folds is less clear than splitting them into their various
components.  Here we have 3:
(1) The "unpack" step @Foldable g => x -> g y@ which maps an item in the initial collection to a foldable of something else.
This encompasses doing nothing (@g ~ Identity@ and @y ~ a@), filtering (@g ~ Maybe@) and something like R's "melt" function where each row
of the input becomes several rows of a different set of columns.
(2) The "assign" step @Ord k => y -> (k, c)@ or @(Hashable k, Eq k => y -> (k, c)@ which assigns each unpacked item to a group, keyed by the type k.  For frames that key will usually, but not always be a subset of your record.
(2a) The "group" step which collects every assigned item by key into a group.
map-reduce-folds is agnostic about how to do the collecting, grouping and what to group into. But the defaults, built into these wrappers,
are to collect into a 'Data.Sequence.Seq', use a map (lazy and strict have similar performance) for the grouping step and to collect like items into lists.
In the general case, we supply a function @Monoid d => c -> d@ to the "gatherer". Here that is specialized to @pure \@[]@. 
(3) The "reduce" step @Monoid e => (k -> [c] -> e)@ which processes each grouped result, in a way which can depend on the value of the key.  The result of this
reduction must be a monoid so that all the results can be packaged up. @e@ could itself be a container type, e.g.,  @[z]@ or @FrameRec xs@ etc.  

We end up with a Control.Foldl.Fold x e.

So, for example, imagine we have a Frame of rows, each having two columns ("Label",Text), ("X",Double) and we want to get the average value in the X column
for every label:

we have (NB: "foldAllConstrained" is from the Frames.Folds module, also in this package and 'mean' is a fold from the Control.Foldl package.)
>>> f1 = mapRListF noUnpack (splitOnKeys @'[Label]) (foldAndAddKey (foldAllConstrained @Num average)) :: Fold (FrameRec OurCols) (FrameRec OurCols)
and performing this fold will give you one row per label, each with the average value of X for rows with that label.

or (using 'filter' from map-reduce-folds)
>>> f2 = mapRListF (filter $ (> 2) . F.rgetField @X) (splitOnKeys @'[Label]) (foldAndAddKey (foldAllConstrained @Num average)) :: Fold (FrameRec OurCols) (FrameRec OurCols)
will do the same, but only consider rows where the value in the X column is > 2.

Because these are folds, they can be combined applicatively so
>> f3 = (,) <$> f1 <*> f2
is also a fold, one that returns both results but only loops over the data once.

If you are combining operations that differ only in the reduce step, you can combine applicatively there as well, saving the work of grouping a second time.  And if both of your reductions are themselves folds, you can loop over each resulting group only once as well, computing both results as you go.

You may notice that the resulting folds are of type @MapFoldT mm x e@.  This is just a wrapper which allows us to
manage non-monadic (@mm@ will be a type-level @Nothing@) and monadic (Monad m => @mm ~ ('Just m)) folds with one type and one
set of functions.  Your unpack and reduce steps must match. So if your reduction step is monadic (uses random numbers, say), then the unpack step will
need to be "generalized" to match and vice-versa.  See map-reduce-folds documentation for details.
-}
module Frames.MapReduce
  (
    -- * unpackers
    unpackFilterRow
  , unpackFilterOnField
  , unpackGoodRows
    -- * key assigners
  , assignKeysAndData
  , assignKeys
  , splitOnKeys
  -- * Frame-specific re-adding of keys
  , reduceAndAddKey
  , foldAndAddKey
  -- * Frame-specific re-adding of keys to mulitple results
  , makeRecsWithKey
  -- * map-reduce-fold specialized to Frames
  , mapReduceGF
  , mapRListF
  , mapRListFOrd
  -- * re-exports
  , module Control.MapReduce
  )
where

import qualified Control.MapReduce             as MR
import           Control.MapReduce

import qualified Control.Foldl                 as FL
import qualified Data.Hashable                 as Hash
import           Data.Monoid                    ( Monoid(..) )
import           Data.Hashable                  ( Hashable )

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

-- | This is only here so we can use hash maps for the grouping step.  This should properly be in Frames itself.
instance Hash.Hashable (F.Record '[]) where
  hash = const 0
  {-# INLINABLE hash #-}
  hashWithSalt s = const s -- TODO: this seems BAD! Or not?
  {-# INLINABLE hashWithSalt #-}

instance (V.KnownField t, Hash.Hashable (V.Snd t), Hash.Hashable (F.Record rs), rs F.⊆ (t ': rs)) => Hash.Hashable (F.Record (t ': rs)) where
  hashWithSalt s r = s `Hash.hashWithSalt` (F.rgetField @t r) `Hash.hashWithSalt` (F.rcast @rs r)
  {-# INLINABLE hashWithSalt #-}

-- | filter rows 
unpackFilterRow
  :: (F.Record rs -> Bool)
  -> MR.Unpack 'Nothing Maybe (F.Record rs) (F.Record rs)
unpackFilterRow test = MR.Unpack $ \rs -> if test rs then Just rs else Nothing

-- | filter rows based on a condition on only one field in the row
unpackFilterOnField
  :: forall t rs
   . (V.KnownField t, F.ElemOf rs t)
  => (V.Snd t -> Bool)
  -> MR.Unpack 'Nothing Maybe (F.Record rs) (F.Record rs)
unpackFilterOnField test = unpackFilterRow (test . F.rgetField @t)

-- | An unpack step which specifies a subset of columns, cs, (via a type-application) and then filters a @Rec (Maybe :. Elfield) rs@
-- to only rows which have all good data in that subset.
unpackGoodRows
  :: forall cs rs
   . (cs F.⊆ rs)
  => MR.Unpack
       'Nothing
       Maybe
       (F.Rec (Maybe F.:. F.ElField) rs)
       (F.Record cs)
unpackGoodRows = MR.Unpack $ F.recMaybe . F.rcast

-- | Assign both keys and data cols.  Uses type applications to specify if they cannot be inferred.  Keys usually can't.  Data often can be from the functions
-- that follow
assignKeysAndData
  :: forall ks cs rs
   . (ks F.⊆ rs, cs F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
assignKeysAndData = MR.assign (F.rcast @ks) (F.rcast @cs)
{-# INLINABLE assignKeysAndData #-}

-- | Assign keys and leave all columns, including the keys, in the data passed to gather and reduce
assignKeys
  :: forall ks rs
   . (ks F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record rs)
assignKeys = MR.assign (F.rcast @ks) id
{-# INLINABLE assignKeys #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to gather and reduce
splitOnKeys
  :: forall ks rs cs
   . (ks F.⊆ rs, cs ~ F.RDeleteAll ks rs, cs F.⊆ rs)
  => MR.Assign (F.Record ks) (F.Record rs) (F.Record cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

-- | The common case where we reduce the data to a single row and then (re-)attach a key
reduceAndAddKey
  :: forall ks cs h x
   . FI.RecVec ((ks V.++ cs))
  => (h x -> F.Record cs) -- ^ reduction step
  -> MR.Reduce 'Nothing (F.Record ks) h x (F.FrameRec (ks V.++ cs))
reduceAndAddKey process =
  fmap (F.toFrame . pure @[]) $ MR.processAndRelabel process V.rappend
{-# INLINABLE reduceAndAddKey #-}

-- | The common case where we reduce (via a fold) the data to a single row and then (re-)attach a key
foldAndAddKey
  :: (Foldable h, FI.RecVec ((ks V.++ cs)))
  => FL.Fold x (F.Record cs) -- ^ reduction fold
  -> MR.Reduce 'Nothing (F.Record ks) h x (F.FrameRec (ks V.++ cs))
foldAndAddKey fld =
  fmap (F.toFrame . pure @[]) $ MR.foldAndRelabel fld V.rappend  -- is Frame a reasonably fast thing for many appends?
{-# INLINABLE foldAndAddKey #-}

-- | Transform a reduce which produces a container of results and a function from each result to a record
-- into a reduce which produces a FrameRec of the result records with the key re-attached
makeRecsWithKey
  :: (Functor g, Foldable g, (FI.RecVec (ks V.++ as)))
  => (y -> F.Record as)
  -> MR.Reduce mm (F.Record ks) h x (g y)
  -> MR.Reduce mm (F.Record ks) h x (F.FrameRec (ks V.++ as))
makeRecsWithKey makeRec reduceToY = fmap F.toFrame
  $ MR.reduceMapWithKey addKey reduceToY
  where addKey k = fmap (V.rappend k . makeRec)
{-# INLINABLE makeRecsWithKey #-}

-- | 'mapReduceGatherFold' specialized to Frames.  Here the 'Gatherer' is left as an input so it can be specified by the user.
mapReduceGF
  :: ( ec e
     , Functor g
     , Functor (MR.MapFoldT mm x)
     , Monoid e
     , Monoid gt
     , Foldable g
     )
  => MR.Gatherer ec gt (F.Record ks) (F.Record cs) [F.Record cs] -- ^ a map-reduce-folds 'Gatherer' for gathering and grouping 
  -> MR.Unpack mm g x y
  -> MR.Assign (F.Record ks) y (F.Record cs)
  -> MR.Reduce mm (F.Record ks) [] (F.Record cs) e
  -> MR.MapFoldT mm x e
mapReduceGF frameGatherer unpacker assigner reducer = MR.mapGatherReduceFold
  (MR.uagMapAllGatherEachFold frameGatherer unpacker assigner)
  reducer

-- | The most common map-reduce form and the simplest to use. Requires @(Hashable (Record ks), Eq (Record ks))@.
-- Note that this is just a less polymorphic version of 'Control.MapReduce.Simple.basicListF`
mapRListF
  :: ( Functor g
     , Functor (MR.MapFoldT mm x)
     , Monoid e
     , Foldable g
     , Hashable (F.Record ks)
     , Eq (F.Record ks)
     )
  => MR.Unpack mm g x y
  -> MR.Assign (F.Record ks) y (F.Record cs)
  -> MR.Reduce mm (F.Record ks) [] (F.Record cs) e
  -> MR.MapFoldT mm x e
mapRListF = MR.basicListF @Hashable --mapReduceGF (MR.defaultHashableGatherer pure)

-- | The most common map-reduce form and the simplest to use. Requires @Ord (Record ks)@
-- Note that this is just a less polymorphic version of 'Control.MapReduce.Simple.basicListF` 
mapRListFOrd
  :: ( Functor g
     , Functor (MR.MapFoldT mm x)
     , Monoid e
     , Foldable g
     , Ord (F.Record ks)
     )
  => MR.Unpack mm g x y
  -> MR.Assign (F.Record ks) y (F.Record cs)
  -> MR.Reduce mm (F.Record ks) [] (F.Record cs) e
  -> MR.MapFoldT mm x e
mapRListFOrd = mapReduceGF (MR.defaultOrdGatherer pure)

{-
-- this is slightly too general to use the above
-- if h x ~ [F.Record as], then these are equivalent
aggregateMonoidalF
  :: forall ks rs as h x cs f g
   . ( ks F.⊆ as
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ cs)
     , Foldable f
     , Functor f
     , Foldable h
     , Monoid (h x)
     )
  => (F.Rec g rs -> f (F.Record as))
  -> (F.Record as -> h x)
  -> (h x -> F.Record cs)
  -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateMonoidalF unpack process extract = MR.mapGatherReduceFold
  (MR.uagMapAllGatherEachFold (MR.gathererSeqToStrictMap process)
                              (MR.Unpack unpack)
                              (assignKeys @ks)
  )
  (reduceAndAddKey extract)
-}
