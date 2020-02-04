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
module Frames.MapReduce.Maybe where

import qualified Control.MapReduce             as MR
import qualified Frames.MapReduce.General      as MG

import qualified Control.Foldl                 as FL
import           Data.Maybe                     ( isJust )

import qualified Frames                        as F
import           Frames                         ( (:.) )
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import           Data.Vinyl                     ( ElField )
--import qualified Data.Vinyl.Functor            as V
import qualified Data.Vinyl.TypeLevel          as V

-- | Don't do anything 
unpackNoOp
  :: MR.Unpack (record (Maybe :. ElField) rs) (record (Maybe :. ElField) rs)
unpackNoOp = MG.unpackNoOp

-- | Filter records using a function on the entire record. 
unpackFilterRow
  :: (record (Maybe :. ElField) rs -> Bool)
  -> MR.Unpack (record (Maybe :. ElField) rs) (record (Maybe :. ElField) rs)
unpackFilterRow = MG.unpackFilterRow

-- | Filter records based on a condition on only one field in the row.  Will usually require a Type Application to indicate which field.
unpackFilterOnField
  :: forall t rs record
   . (V.KnownField t, F.ElemOf rs t, MG.RecGetFieldC t record Maybe rs)
  => (Maybe (V.Snd t) -> Bool)
  -> MR.Unpack
       (record (Maybe :. ElField) rs)
       (record (Maybe :. ElField) rs)
unpackFilterOnField = MG.unpackFilterOnField @t

-- | An unpack step which specifies a subset of columns, cs,
-- (via a type-application) and then filters a @Rec (Maybe :. Elfield) rs@
-- to only rows which have all good data in that subset.
unpackGoodRows
  :: forall cs rs record
   . (MG.RCastC cs rs record Maybe)
  => (record (Maybe :. ElField) cs -> Bool)
  -> MR.Unpack
       (record (Maybe :. ElField) rs)
       (record (Maybe :. ElField) rs)
unpackGoodRows = MG.unpackGoodRows  --unpackFilterRow (isJust . F.recMaybe . F.rcast @cs)

unpackGoodRecRows
  :: forall cs rs
   . (MG.RCastC cs rs V.Rec Maybe)
  => MR.Unpack (V.Rec (Maybe :. ElField) rs) (V.Rec (Maybe :. ElField) rs)
unpackGoodRecRows = MG.unpackGoodRows @cs (isJust . F.recMaybe)

-- | Assign both keys and data cols.  Uses type applications to specify them if they cannot be inferred.
-- Keys usually can't. Data sometimes can.
assignKeysAndData
  :: forall ks cs rs record
   . (MG.RCastC ks rs record Maybe, MG.RCastC cs rs record Maybe)
  => MR.Assign
       (record (Maybe :. ElField) ks)
       (record (Maybe :. ElField) rs)
       (record (Maybe :. ElField) cs)
assignKeysAndData = MG.assignKeysAndData --MR.assign (F.rcast @ks) (F.rcast @cs)
{-# INLINABLE assignKeysAndData #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to reduce.
splitOnKeys
  :: forall ks rs cs record
   . ( MG.RCastC ks rs record Maybe
     , MG.RCastC cs rs record Maybe
     , cs ~ F.RDeleteAll ks rs
     )
  => MR.Assign
       (record (Maybe :. ElField) ks)
       (record (Maybe :. ElField) rs)
       (record (Maybe :. ElField) cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to reduce.
splitOnData
  :: forall cs rs ks record
   . ( MG.RCastC ks rs record Maybe
     , MG.RCastC cs rs record Maybe
     , ks ~ F.RDeleteAll cs rs
     )
  => MR.Assign
       (record (Maybe :. ElField) ks)
       (record (Maybe :. ElField) rs)
       (record (Maybe :. ElField) cs)
splitOnData = assignKeysAndData @ks @cs
{-# INLINABLE splitOnData #-}


-- | Assign keys and leave all columns, including the keys, in the data passed to reduce.
assignKeys
  :: forall ks rs record
   . (MG.RCastC ks rs record Maybe)
  => MR.Assign
       (record (Maybe :. ElField) ks)
       (record (Maybe :. ElField) rs)
       (record (Maybe :. ElField) rs)
assignKeys = MG.assignKeys
{-# INLINABLE assignKeys #-}

-- | Reduce the data to a single row and then re-attach the key.
reduceAndAddKey
  :: forall ks cs x record
   . ( MG.IsoRec ks record Maybe
     , MG.IsoRec cs record Maybe
     , MG.IsoRec (ks V.++ cs) record Maybe
     , FI.RecVec ((ks V.++ cs))
     )
  => (forall h . Foldable h => h x -> record (Maybe :. ElField) cs) -- ^ reduction step
  -> MR.Reduce
       (record (Maybe :. ElField) ks)
       x
       (record (Maybe :. ElField) (ks V.++ cs))
reduceAndAddKey = MG.reduceAndAddKey
{-# INLINABLE reduceAndAddKey #-}

-- | Reduce by folding the data to a single row and then re-attaching the key.
foldAndAddKey
  :: ( MG.IsoRec ks record Maybe
     , MG.IsoRec cs record Maybe
     , MG.IsoRec (ks V.++ cs) record Maybe
     , FI.RecVec ((ks V.++ cs))
     )
  => FL.Fold x (record (Maybe :. ElField) cs) -- ^ reduction fold
  -> MR.Reduce
       (record (Maybe :. ElField) ks)
       x
       (record (Maybe :. ElField) (ks V.++ cs))
foldAndAddKey = MG.foldAndAddKey
{-# INLINABLE foldAndAddKey #-}

-- | Transform a reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKey
  :: ( Functor g
     , Foldable g
     , MG.IsoRec ks record Maybe
     , MG.IsoRec as record Maybe
     , MG.IsoRec (ks V.++ as) record Maybe
     , (FI.RecVec (ks V.++ as))
     )
  => (y -> record (Maybe :. ElField) as) -- ^ map a result to a record
  -> MR.Reduce (record (Maybe :. ElField) ks) x (g y) -- ^ original reduce
  -> MR.Reduce
       (record (Maybe :. ElField) ks)
       x
       (g (record (Maybe :. ElField) (ks V.++ as)))
makeRecsWithKey = MG.makeRecsWithKey
{-# INLINABLE makeRecsWithKey #-}

-- | Transform an effectful reduce which produces a container of results, with a function from each result to a record,
-- into a reduce which produces a FrameRec of the result records with the key re-attached.
makeRecsWithKeyM
  :: ( Monad m
     , Functor g
     , Foldable g
     , MG.IsoRec ks record Maybe
     , MG.IsoRec as record Maybe
     , MG.IsoRec (ks V.++ as) record Maybe
     , (FI.RecVec (ks V.++ as))
     )
  => (y -> record (Maybe :. ElField) as) -- ^ map a result to a record
  -> MR.ReduceM m (record (Maybe :. ElField) ks) x (g y) -- ^ original reduce
  -> MR.ReduceM
       m
       (record (Maybe :. ElField) ks)
       x
       (g (record (Maybe :. ElField) (ks V.++ as)))
makeRecsWithKeyM = MG.makeRecsWithKeyM
{-# INLINABLE makeRecsWithKeyM #-}
