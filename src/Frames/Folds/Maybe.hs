{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Frames.Folds.Maybe
Description : Types and functions to simplify folding over Vinyl/Frames records. Leans heavily on the foldl package. 
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Frames.Folds contains various helper functions designed to simplify folding over Frames/Vinyl records given some way of folding over each column.
-}
module Frames.Folds.Maybe
  (
    -- * Types
    EndoFold

    -- ** Types to act as "interpretation functors" for records of folds
  , FoldEndo(..)
  , FoldRecord(..)

  -- * functions for building records of folds
  , recFieldF
  , fieldToFieldFold

  -- * functions for turning records of folds into folds of records
  , sequenceRecFold
  , sequenceEndoFolds

  -- * functions using constraints to extend an endo-fold across a record
  , foldAll
  , foldAllConstrained
  , maybeFoldAllConstrained
  , foldAllMonoid
  )
where

import qualified Frames.MapReduce.General      as MG
import qualified Frames.Folds.General          as FG
import           Frames.Folds.General           ( FoldEndo(..)
                                                , FoldRecord(..)
                                                )

import           Frames.Folds                   ( EndoFold
                                                , FoldFieldEndo(..)
                                                , monoidWrapperToFold
                                                , MonoidalField
                                                )

import qualified Control.Foldl                 as FL

import qualified Data.Profunctor               as P
import qualified Data.Vinyl                    as V
import           Data.Vinyl                     ( ElField )
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import           Frames                         ( (:.) )
import qualified Frames.Melt                   as F

-- | Helper for building a 'FoldRecord' from a given fold and function of the record
recFieldF
  :: forall t rs a record
   . V.KnownField t
  => FL.Fold a (V.Snd t) -- ^ A fold from some type a to the field type of an ElField 
  -> (record (Maybe :. ElField) rs -> Maybe a) -- ^ a function to get the a value from the input record
  -> FG.FoldRecord record Maybe (Maybe :. ElField) rs t -- ^ the resulting 'FoldRecord'-wrapped fold 
recFieldF = FG.recFieldF id
{-# INLINABLE recFieldF #-}

-- | special case of 'recFieldF' for the case when the function from the record to the folded type
-- is just retrieving the value in a field.
fieldToFieldFold
  :: forall x y rs record
   . ( V.KnownField x
     , V.KnownField y
     , F.ElemOf rs x
     , MG.RecGetFieldC x record Maybe rs
     )
  => FL.Fold (V.Snd x) (V.Snd y) -- ^ the fold to be wrapped
  -> FG.FoldRecord record Maybe (Maybe :. ElField) rs y -- ^ the wrapped fold
fieldToFieldFold = FG.fieldToFieldFold @x id
{-# INLINABLE fieldToFieldFold #-}

-- can we do all/some of this via F.Rec (Fold as) bs?
-- | Turn a Record of folds into a fold over records
sequenceRecFold
  :: forall as rs record
   . (MG.IsoRec rs record Maybe)
  => F.Rec (FG.FoldRecord record Maybe (Maybe :. ElField) as) rs
  -> FL.Fold (record (Maybe :. ElField) as) (record (Maybe :. ElField) rs)
sequenceRecFold = FG.sequenceRecFold --V.rtraverse unFoldRecord
{-# INLINABLE sequenceRecFold #-}

-- | turn a record of endo-folds over each field, into a fold over records 
sequenceEndoFolds
  :: forall rs record
   . ( V.RApply rs
     , V.RPureConstrained V.KnownField rs
     , FG.EndoFieldFoldsToRecordFolds rs record Maybe
     , MG.IsoRec rs record Maybe
     )
  => F.Rec (FG.FoldEndo Maybe) rs
  -> FL.Fold (record (Maybe :. ElField) rs) (record (Maybe :. ElField) rs)
sequenceEndoFolds = FG.sequenceEndoFolds --sequenceFieldEndoFolds . liftFolds
{-# INLINABLE sequenceEndoFolds #-}

-- | apply an unconstrained endo-fold, e.g., a fold which takes the last item in a container, to every field in a record
foldAll
  :: ( V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , FG.EndoFieldFoldsToRecordFolds rs record Maybe
     , MG.IsoRec rs record Maybe
     )
  => (forall a . FL.Fold a a)
  -> FL.Fold (record (Maybe :. ElField) rs) (record (Maybe :. ElField) rs)
foldAll = FG.foldAll
{-# INLINABLE foldAll #-}

-- | Apply a constrained endo-fold to all fields of a record.
-- May require a use of TypeApplications, e.g., foldAllConstrained @Num FL.sum
foldAllConstrained
  :: forall c rs record
   . ( V.RPureConstrained (FG.ConstrainedField c) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , FG.EndoFieldFoldsToRecordFolds rs record Maybe
     , MG.IsoRec rs record Maybe
     )
  => (forall a . c a => FL.Fold a a)
  -> FL.Fold (record (Maybe :. ElField) rs) (record (Maybe :. ElField) rs)
foldAllConstrained = FG.foldAllConstrained @c id
{-# INLINABLE foldAllConstrained #-}

maybeFoldAllConstrained
  :: forall c rs record
   . ( V.RPureConstrained (FG.ConstrainedField c) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , FG.EndoFieldFoldsToRecordFolds rs record Maybe
     , MG.IsoRec rs record Maybe
     )
  => (forall a . c a => FL.Fold (Maybe a) (Maybe a))
  -> FL.Fold (record (Maybe :. ElField) rs) (record (Maybe :. ElField) rs)
maybeFoldAllConstrained = FG.functorFoldAllConstrained @c
{-# INLINABLE maybeFoldAllConstrained #-}

-- | Given a monoid-wrapper, e.g., Sum, apply the derived endo-fold to all fields of a record
-- This is strictly less powerful than foldAllConstrained but might be simpler to use in some cases
foldAllMonoid
  :: forall g rs record
   . ( V.RPureConstrained (FG.ConstrainedField (MonoidalField g)) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , FG.EndoFieldFoldsToRecordFolds rs record Maybe
     , MG.IsoRec rs record Maybe
     )
  => FL.Fold (record (Maybe :. ElField) rs) (record (Maybe :. ElField) rs)
foldAllMonoid = FG.foldAllMonoid @g id
{-# INLINABLE foldAllMonoid #-}
