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


{-
rgetFieldG
  :: forall f t rs
   . (V.KnownField t, F.ElemOf rs t, V.FieldType (V.Fst t) rs ~ V.Snd t)
  => (forall f rs. rt f 
  -> rt (f :. ElField) rs
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

data RGetField t rt f where
  RGetField :: (V.KnownField t, F.ElemOf rs t, Functor f) => (rt (f :. ElField) rs -> f (V.Snd t)) -> RGetField t rt f

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

class RecGetFieldC t rt f where
  rgetFieldF :: forall rs. ( V.KnownField t
                           , Functor f
                           , F.ElemOf rs t
                           ) => rt (f :. ElField) rs -> f (V.Snd t)

{-
type family RemoveIdentity (a :: Type -> Type) :: Type -> Type
type instance RemoveIdentity (Identity :. ElField) = ElField
type instance RemoveIdentity a = a
-}

-- | This is only here so we can use hash maps for the grouping step.  This should properly be in Frames itself.
instance Hash.Hashable (rt (f :. ElField)  '[]) where
  hash = const 0
  {-# INLINABLE hash #-}
  hashWithSalt s = const s -- TODO: this seems BAD! Or not?
  {-# INLINABLE hashWithSalt #-}

instance (V.KnownField t
         , Functor f
         , RecGetFieldC t rt f
         , Hash.Hashable (f (V.Snd t))
         , Hash.Hashable (rt (f :. ElField) rs)
         , rs F.⊆ (t ': rs)) => Hash.Hashable (rt (f :. ElField) (t ': rs)) where
  hashWithSalt s r = s `Hash.hashWithSalt` (rgetFieldF @t r) `Hash.hashWithSalt` (F.rcast @rs r)
  {-# INLINABLE hashWithSalt #-}
{-
-- | Don't do anything 
unpackNoOp
  :: MR.Unpack (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
unpackNoOp = MR.Filter (const True)

-- | Filter records using a function on the entire record. 
unpackFilterRow
  :: (F.Rec (Maybe :. ElField) rs -> Bool)
  -> MR.Unpack (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
unpackFilterRow test = MR.Filter test

-- | Filter records based on a condition on only one field in the row.  Will usually require a Type Application to indicate which field.
unpackFilterOnField
  :: forall t rs
   . (V.KnownField t, F.ElemOf rs t, V.FieldType (V.Fst t) rs ~ V.Snd t)
  => (Maybe (V.Snd t) -> Bool)
  -> MR.Unpack (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
unpackFilterOnField test = unpackFilterRow (test . rgetMaybeField @t)

-- | An unpack step which specifies a subset of columns, cs,
-- (via a type-application) and then filters a @Rec (Maybe :. Elfield) rs@
-- to only rows which have all good data in that subset.
unpackGoodRows
  :: forall cs rs
   . (cs F.⊆ rs)
  => MR.Unpack (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
unpackGoodRows = unpackFilterRow (isJust . F.recMaybe . F.rcast @cs)

-- | Assign both keys and data cols.  Uses type applications to specify them if they cannot be inferred.
-- Keys usually can't. Data sometimes can.
assignKeysAndData
  :: forall ks cs rs
   . (ks F.⊆ rs, cs F.⊆ rs)
  => MR.Assign
       (F.Rec (Maybe :. ElField) ks)
       (F.Rec (Maybe :. ElField) rs)
       (F.Rec (Maybe :. ElField) cs)
assignKeysAndData = MR.assign (F.rcast @ks) (F.rcast @cs)
{-# INLINABLE assignKeysAndData #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to reduce.
splitOnKeys
  :: forall ks rs cs
   . (ks F.⊆ rs, cs ~ F.RDeleteAll ks rs, cs F.⊆ rs)
  => MR.Assign
       (F.Rec (Maybe :. ElField) ks)
       (F.Rec (Maybe :. ElField) rs)
       (F.Rec (Maybe :. ElField) cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

-- | Assign keys and leave all columns, including the keys, in the data passed to reduce.
assignKeys
  :: forall ks rs
   . (ks F.⊆ rs)
  => MR.Assign
       (F.Rec (Maybe :. ElField) ks)
       (F.Rec (Maybe :. ElField) rs)
       (F.Rec (Maybe :. ElField) rs)
assignKeys = MR.assign (F.rcast @ks) id
{-# INLINABLE assignKeys #-}

-- | Reduce the data to a single row and then re-attach the key.
reduceAndAddKey
  :: forall ks cs x
   . FI.RecVec ((ks V.++ cs))
  => (forall h . Foldable h => h x -> F.Rec (Maybe :. ElField) cs) -- ^ reduction step
  -> MR.Reduce
       (F.Rec (Maybe :. ElField) ks)
       x
       (F.Rec (Maybe :. ElField) (ks V.++ cs))
reduceAndAddKey process = MR.processAndLabel process V.rappend
{-# INLINABLE reduceAndAddKey #-}

-- | Reduce by folding the data to a single row and then re-attaching the key.
foldAndAddKey
  :: (FI.RecVec ((ks V.++ cs)))
  => FL.Fold x (F.Rec (Maybe :. ElField) cs) -- ^ reduction fold
  -> MR.Reduce
       (F.Rec (Maybe :. ElField) ks)
       x
       (F.Rec (Maybe :. ElField) (ks V.++ cs))
foldAndAddKey fld = MR.foldAndLabel fld V.rappend
{-# INLINABLE foldAndAddKey #-}

-- | Transform a reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKey
  :: (Functor g, Foldable g, (FI.RecVec (ks V.++ as)))
  => (y -> F.Rec (Maybe :. ElField) as) -- ^ map a result to a record
  -> MR.Reduce (F.Rec (Maybe :. ElField) ks) x (g y) -- ^ original reduce
  -> MR.Reduce
       (F.Rec (Maybe :. ElField) ks)
       x
       (g (F.Rec (Maybe :. ElField) (ks V.++ as)))
makeRecsWithKey makeRec reduceToY = MR.reduceMapWithKey addKey reduceToY
  where addKey k = fmap (V.rappend k . makeRec)
{-# INLINABLE makeRecsWithKey #-}

-- | Transform an effectful reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKeyM
  :: (Monad m, Functor g, Foldable g, (FI.RecVec (ks V.++ as)))
  => (y -> F.Rec (Maybe :. ElField) as) -- ^ map a result to a record
  -> MR.ReduceM m (F.Rec (Maybe :. ElField) ks) x (g y) -- ^ original reduce
  -> MR.ReduceM
       m
       (F.Rec (Maybe :. ElField) ks)
       x
       (g (F.Rec (Maybe :. ElField) (ks V.++ as)))
makeRecsWithKeyM makeRec reduceToY = MR.reduceMMapWithKey addKey reduceToY
  where addKey k = fmap (V.rappend k . makeRec)
{-# INLINABLE makeRecsWithKeyM #-}
-}
