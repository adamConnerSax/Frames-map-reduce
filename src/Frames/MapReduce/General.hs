{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE CPP                   #-}
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
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Frames.MapReduce.General where

import qualified Control.MapReduce             as MR

import qualified Control.Foldl                 as FL
import qualified Data.Hashable                 as Hash
import           Data.Kind                      ( Type )
import           GHC.TypeLits                   ( Symbol )

import           Frames                         ( (:.) )
import qualified Frames.Melt                   as F
import qualified Data.Vinyl                    as V
import           Data.Vinyl                     ( ElField )
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.SRec               as V
import qualified Data.Vinyl.ARec               as V
import qualified Foreign.Storable              as FS

class RecGetFieldC t record f rs where
  rgetF ::  ( V.KnownField t
            , F.ElemOf rs t
            ) => record (f :. ElField) rs -> (f :. ElField) t
  rgetFieldF :: ( V.KnownField t
                , Functor f
                , F.ElemOf rs t
                ) => record (f :. ElField) rs -> f (V.Snd t)
  rgetFieldF = fmap V.getField . V.getCompose . rgetF @t @record @f @rs

instance RecGetFieldC t V.Rec f rs where
  rgetF = V.rget @t

instance RecGetFieldC t V.ARec f rs where
  rgetF = V.aget @t

instance (V.FieldOffset (f :. ElField) rs t) => RecGetFieldC t V.SRec f rs where
  rgetF = V.sget @_ @t . V.getSRecNT --srecGetField @t @f @rs

class RCastC rs ss record f  where
  rcastF :: record (f :. ElField) ss -> record (f :. ElField) rs

instance V.RecSubset V.Rec rs ss (V.RImage rs ss) => RCastC rs ss V.Rec f where
  rcastF = V.rcast

instance (V.IndexWitnesses (V.RImage rs ss), V.NatToInt (V.RLength rs)) => RCastC rs ss V.ARec f where
  rcastF = V.arecGetSubset

instance (V.RPureConstrained (V.FieldOffset (f :. ElField) ss) rs
         , V.RPureConstrained (V.FieldOffset (f :. ElField) rs) rs
         , V.RFoldMap rs
         , V.RMap rs
         , V.RApply rs
         , FS.Storable (V.Rec (f :. ElField) rs)) =>  RCastC rs ss V.SRec f where
  rcastF = V.SRecNT . V.srecGetSubset . V.getSRecNT

class IsoRec rs record f where
  toRec :: record (f :. ElField) rs -> V.Rec (f :. ElField) rs
  fromRec :: V.Rec (f :. ElField) rs -> record (f :. ElField) rs

instance IsoRec rs V.Rec f where
  toRec = id
  fromRec = id

instance FS.Storable (V.Rec (f :. ElField) rs) => IsoRec rs V.SRec f where
  toRec = V.fromSRec
  fromRec = V.toSRec


instance (V.NatToInt (V.RLength rs)
         , V.RecApplicative rs
         , V.RPureConstrained (V.IndexableField rs) rs
#if MIN_VERSION_vinyl(0,14,2)
         , V.ToARec rs
#endif
         ) => IsoRec rs V.ARec f where
  toRec = V.fromARec
  fromRec = V.toARec

isoRecAppend
  :: forall f record (as :: [(Symbol, Type)]) bs
   . (IsoRec as record f, IsoRec bs record f, IsoRec (as V.++ bs) record f)
  => record (f :. ElField) as
  -> record (f :. ElField) bs
  -> record (f :. ElField) (as V.++ bs)
isoRecAppend lhs rhs =
  fromRec @(as V.++ bs) @record @f
    $           (toRec @as @record @f lhs)
    `V.rappend` (toRec @bs @record @f rhs)

-- | This is only here so we can use hash maps for the grouping step.  This should properly be in Frames itself.
#if MIN_VERSION_hashable(1,4,0)
instance Eq (record (f :. ElField) '[]) => Hash.Hashable (record (f :. ElField)  '[]) where
#else
instance Hash.Hashable (record (f :. ElField)  '[]) where
#endif
  hash = const 0
  {-# INLINABLE hash #-}
  hashWithSalt s = const s -- TODO: this seems BAD! Or not?
  {-# INLINABLE hashWithSalt #-}


instance (V.KnownField t
         , Functor f
         , RecGetFieldC t record f (t ': rs)
         , RCastC rs (t ': rs) record f
#if  MIN_VERSION_hashable(1,4,0)
         , Eq (record (f :. ElField) (t ': rs))
#endif
         , Hash.Hashable (f (V.Snd t))
         , Hash.Hashable (record (f :. ElField) rs)
         ) => Hash.Hashable (record (f :. ElField) (t ': rs)) where
  hashWithSalt s r = s `Hash.hashWithSalt` (rgetFieldF @t r) `Hash.hashWithSalt` (rcastF @rs r)
  {-# INLINABLE hashWithSalt #-}

-- | Don't do anything
unpackNoOp :: MR.Unpack (record (f :. ElField) rs) (record (f :. ElField) rs)
unpackNoOp = MR.Filter (const True)

-- | Filter records using a function on the entire record.
unpackFilterRow
  :: (record (f :. ElField) rs -> Bool)
  -> MR.Unpack (record (f :. ElField) rs) (record (f :. ElField) rs)
unpackFilterRow test = MR.Filter test

-- | Filter records based on a condition on only one field in the row.  Will usually require a Type Application to indicate which field.
unpackFilterOnField
  :: forall t rs record f
   . (Functor f, V.KnownField t, F.ElemOf rs t, RecGetFieldC t record f rs)
  => (f (V.Snd t) -> Bool)
  -> MR.Unpack (record (f :. ElField) rs) (record (f :. ElField) rs)
unpackFilterOnField test = unpackFilterRow (test . rgetFieldF @t)

unpackFilterOnGoodField
  :: forall t rs record f
   . (Functor f, V.KnownField t, F.ElemOf rs t, RecGetFieldC t record f rs)
  => (forall a . f a -> Maybe a)
  -> (V.Snd t -> Bool)
  -> MR.Unpack (record (f :. ElField) rs) (record (f :. ElField) rs)
unpackFilterOnGoodField toMaybe testValue =
  let test' = (maybe False testValue) . toMaybe in unpackFilterOnField @t test'

-- | An unpack step which specifies a subset of columns, cs,
-- (via a type-application) and then filters a @record (Maybe :. Elfield) rs@
-- to only rows which have all good data in that subset.
unpackGoodRows
  :: forall cs rs record f
   . (RCastC cs rs record f)
  => (record (f :. ElField) cs -> Bool)
  -> MR.Unpack (record (f :. ElField) rs) (record (f :. ElField) rs)
unpackGoodRows testSubset = unpackFilterRow (testSubset . rcastF @cs)

-- | Assign both keys and data cols.  Uses type applications to specify them if they cannot be inferred.
-- Keys usually can't. Data sometimes can.
assignKeysAndData
  :: forall ks cs rs record f
   . (RCastC ks rs record f, RCastC cs rs record f)
  => MR.Assign
       (record (f :. ElField) ks)
       (record (f :. ElField) rs)
       (record (f :. ElField) cs)
assignKeysAndData = MR.assign (rcastF @ks) (rcastF @cs)
{-# INLINABLE assignKeysAndData #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to reduce.
splitOnKeys
  :: forall ks rs cs record f
   . (RCastC ks rs record f, RCastC cs rs record f, cs ~ F.RDeleteAll ks rs)
  => MR.Assign
       (record (f :. ElField) ks)
       (record (f :. ElField) rs)
       (record (f :. ElField) cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

-- | Assign data and leave the rest of the columns, excluding the data, as the keys.
splitOnData
  :: forall cs rs ks record f
   . (RCastC ks rs record f, RCastC cs rs record f, ks ~ F.RDeleteAll cs rs)
  => MR.Assign
       (record (f :. ElField) ks)
       (record (f :. ElField) rs)
       (record (f :. ElField) cs)
splitOnData = assignKeysAndData @ks @cs
{-# INLINABLE splitOnData #-}


-- | Assign keys and leave all columns, including the keys, in the data passed to reduce.
assignKeys
  :: forall ks rs record f
   . (RCastC ks rs record f)
  => MR.Assign
       (record (f :. ElField) ks)
       (record (f :. ElField) rs)
       (record (f :. ElField) rs)
assignKeys = MR.assign (rcastF @ks) id
{-# INLINABLE assignKeys #-}

-- | Reduce the data to a single row and then re-attach the key.
-- | NB: for all but Rec case, this will have to convert record to Rec and back for the append
reduceAndAddKey
  :: forall ks cs x record f
   . (IsoRec ks record f, IsoRec cs record f, IsoRec (ks V.++ cs) record f)
  => (forall h . Foldable h => h x -> record (f :. ElField) cs) -- ^ reduction step
  -> MR.Reduce
       (record (f :. ElField) ks)
       x
       (record (f :. ElField) (ks V.++ cs))
reduceAndAddKey process =
  MR.processAndLabel process (\k y -> fromRec (toRec k `V.rappend` toRec y))
{-# INLINABLE reduceAndAddKey #-}

-- | Reduce by folding the data to a single row and then re-attaching the key.
foldAndAddKey
  :: (IsoRec ks record f, IsoRec cs record f, IsoRec (ks V.++ cs) record f)
  => FL.Fold x (record (f :. ElField) cs) -- ^ reduction fold
  -> MR.Reduce
       (record (f :. ElField) ks)
       x
       (record (f :. ElField) (ks V.++ cs))
foldAndAddKey fld =
  MR.foldAndLabel fld (\k y -> fromRec (toRec k `V.rappend` toRec y))
{-# INLINABLE foldAndAddKey #-}

-- | Transform a reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a foldable (based on the original reduce) of the result records with the key re-attached.
makeRecsWithKey
  :: ( Functor g
     , Foldable g
     , IsoRec ks record f
     , IsoRec as record f
     , IsoRec (ks V.++ as) record f
     )
  => (y -> record (f :. ElField) as) -- ^ map a result to a record
  -> MR.Reduce (record (f :. ElField) ks) x (g y) -- ^ original reduce
  -> MR.Reduce
       (record (f :. ElField) ks)
       x
       (g (record (f :. ElField) (ks V.++ as)))
makeRecsWithKey makeRec reduceToY = MR.reduceMapWithKey addKey reduceToY
 where
  addKey k = fmap (\y -> fromRec . V.rappend (toRec k) . toRec $ makeRec y)
{-# INLINABLE makeRecsWithKey #-}

-- | Transform an effectful reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a foldable (based on the original reduce) of the result records with the key re-attached.
makeRecsWithKeyM
  :: ( Monad m
     , Functor g
     , Foldable g
     , IsoRec ks record f
     , IsoRec as record f
     , IsoRec (ks V.++ as) record f
     )
  => (y -> record (f :. ElField) as) -- ^ map a result to a record
  -> MR.ReduceM m (record (f :. ElField) ks) x (g y) -- ^ original reduce
  -> MR.ReduceM
       m
       (record (f :. ElField) ks)
       x
       (g (record (f :. ElField) (ks V.++ as)))
makeRecsWithKeyM makeRec reduceToY = MR.reduceMMapWithKey addKey reduceToY
 where
  addKey k = fmap (\y -> fromRec . V.rappend (toRec k) . toRec $ makeRec y)
{-# INLINABLE makeRecsWithKeyM #-}
