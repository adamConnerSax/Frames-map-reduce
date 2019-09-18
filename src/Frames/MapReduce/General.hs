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
module Frames.MapReduce.General where
import qualified Control.MapReduce             as MR
import           Control.MapReduce                 -- for re-export

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

{-
rgetFieldG
  :: forall f t rs
   . (V.KnownField t, F.ElemOf rs t, V.FieldType (V.Fst t) rs ~ V.Snd t)
  => (forall f rs. record f 
  -> record (f :. ElField) rs
  -> f (V.Snd t)
rgetFieldG = fmap V.getField . V.getCompose . V.rgetf (V.Label @(V.Fst t))
-}

recGetField
  :: forall t f rs
   . (V.KnownField t, F.ElemOf rs t, Functor f)
  => V.Rec (f :. ElField) rs
  -> f (V.Snd t)
recGetField = fmap V.getField . V.getCompose . V.rget @t -- (V.Label @(V.Fst t))

arecGetField
  :: forall t f rs
   . (V.KnownField t, F.ElemOf rs t, Functor f)
  => V.ARec (f :. ElField) rs
  -> f (V.Snd t)
arecGetField = fmap V.getField . V.getCompose . V.aget @t --(V.Label @(V.Fst t))

srecGetField
  :: forall t (f :: Type -> Type) rs
   . ( V.KnownField t
     , F.ElemOf rs t
     , V.FieldOffset (f :. ElField) rs t
     , Functor f
     )
  => V.SRec (f :. ElField) rs
  -> f (V.Snd t)
srecGetField = fmap V.getField . V.getCompose . V.sget @_ @t . V.getSRecNT --(V.Label @(V.Fst t))

data RGetField t record f where
  RGetField :: (V.KnownField t, F.ElemOf rs t, Functor f) => (record (f :. ElField) rs -> f (V.Snd t)) -> RGetField t record f

recGetFieldF
  :: forall t f rs
   . (V.KnownField t, Functor f, F.ElemOf rs t)
  => RGetField t V.Rec f
recGetFieldF = RGetField (recGetField @t @f @rs)

arecGetFieldF
  :: forall t f rs
   . (V.KnownField t, Functor f, F.ElemOf rs t)
  => RGetField t V.ARec f
arecGetFieldF = RGetField (arecGetField @t @f @rs)

srecGetFieldF
  :: forall t f rs
   . ( V.KnownField t
     , Functor f
     , F.ElemOf rs t
     , V.FieldOffset (f :. ElField) rs t
     )
  => RGetField t V.SRec f
srecGetFieldF = RGetField (srecGetField @t @f @rs)

class RecGetFieldC t record f rs where
  rgetFieldF :: ( V.KnownField t
                , Functor f
                , F.ElemOf rs t
                ) => record (f :. ElField) rs -> f (V.Snd t)


instance RecGetFieldC t V.Rec f rs where
  rgetFieldF = recGetField @t

instance RecGetFieldC t V.ARec f rs where
  rgetFieldF = arecGetField @t

instance (V.FieldOffset (f :. ElField) rs t) => RecGetFieldC t V.SRec f rs where
  rgetFieldF = srecGetField @t @f @rs

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
         , V.RPureConstrained (V.IndexableField rs) rs) => IsoRec rs V.ARec f where
  toRec = V.fromARec
  fromRec = V.toARec
{-
type family RemoveIdentity (a :: Type -> Type) :: Type -> Type
type instance RemoveIdentity (Identity :. ElField) = ElField
type instance RemoveIdentity a = a
-}

-- | This is only here so we can use hash maps for the grouping step.  This should properly be in Frames itself.
instance Hash.Hashable (record (f :. ElField)  '[]) where
  hash = const 0
  {-# INLINABLE hash #-}
  hashWithSalt s = const s -- TODO: this seems BAD! Or not?
  {-# INLINABLE hashWithSalt #-}

instance (V.KnownField t
         , Functor f
         , RecGetFieldC t record f (t ': rs)
         , RCastC rs (t ': rs) record f
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
   . ( IsoRec ks record f
     , IsoRec cs record f
     , IsoRec (ks V.++ cs) record f
     , FI.RecVec ((ks V.++ cs))
     )
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
  :: ( IsoRec ks record f
     , IsoRec cs record f
     , IsoRec (ks V.++ cs) record f
     , FI.RecVec ((ks V.++ cs))
     )
  => FL.Fold x (F.Rec (Maybe :. ElField) cs) -- ^ reduction fold
  -> MR.Reduce
       (F.Rec (Maybe :. ElField) ks)
       x
       (F.Rec (Maybe :. ElField) (ks V.++ cs))
foldAndAddKey fld =
  MR.foldAndLabel fld (\k y -> fromRec (toRec k `V.rappend` toRec y))
{-# INLINABLE foldAndAddKey #-}

-- | Transform a reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKey
  :: ( Functor g
     , Foldable g
     , IsoRec ks record f
     , IsoRec as record f
     , IsoRec (ks V.++ as) record f
     , (FI.RecVec (ks V.++ as))
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
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKeyM
  :: ( Monad m
     , Functor g
     , Foldable g
     , IsoRec ks record f
     , IsoRec as record f
     , IsoRec (ks V.++ as) record f
     , (FI.RecVec (ks V.++ as))
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


