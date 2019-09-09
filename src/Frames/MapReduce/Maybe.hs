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
module Frames.MapReduce.Maybe where 
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

-- | Don't do anything 
unpackNoOp
  :: MR.Unpack (F.Rec (Maybe F.:. F.ElField ) rs) (F.Rec (Maybe F.:. F.ElField ) rs)
unpackNoOp = MR.Filter (const True)

-- | Assign both keys and data cols.  Uses type applications to specify them if they cannot be inferred.
-- Keys usually can't. Data sometimes can.
assignKeysAndData
  :: forall ks cs rs
   . (ks F.⊆ rs, cs F.⊆ rs)
  => MR.Assign (F.Rec (Maybe F.:. F.ElField ) ks) (F.Rec (Maybe F.:. F.ElField ) rs) (F.Rec (Maybe F.:. F.ElField )cs)
assignKeysAndData = MR.assign (F.rcast @ks) (F.rcast @cs)
{-# INLINABLE assignKeysAndData #-}


-- | Assign keys and leave the rest of the columns, excluding the keys, in the data passed to reduce.
splitOnKeys
  :: forall ks rs cs
   . (ks F.⊆ rs, cs ~ F.RDeleteAll ks rs, cs F.⊆ rs)
  => MR.Assign (F.Rec (Maybe F.:. F.ElField ) ks) (F.Rec (Maybe F.:. F.ElField ) rs) (F.Rec (Maybe F.:. F.ElField ) cs)
splitOnKeys = assignKeysAndData @ks @cs
{-# INLINABLE splitOnKeys #-}

-- | Assign keys and leave all columns, including the keys, in the data passed to reduce.
assignKeys
  :: forall ks rs
   . (ks F.⊆ rs)
  => MR.Assign (F.Rec (Maybe F.:. F.ElField ) ks) (F.Rec (Maybe F.:. F.ElField ) rs) (F.Rec (Maybe F.:. F.ElField ) rs)
assignKeys = MR.assign (F.rcast @ks) id
{-# INLINABLE assignKeys #-}

-- | Reduce by folding the data to a single row and then re-attaching the key.
foldAndAddKey
  :: (FI.RecVec ((ks V.++ cs)))
  => FL.Fold x (F.Rec (Maybe F.:. F.ElField ) cs) -- ^ reduction fold
  -> MR.Reduce (F.Rec (Maybe F.:. F.ElField ) ks) x (F.Rec (Maybe F.:. F.ElField ) (ks V.++ cs))
foldAndAddKey fld = MR.foldAndLabel fld V.rappend 
{-# INLINABLE foldAndAddKey #-}