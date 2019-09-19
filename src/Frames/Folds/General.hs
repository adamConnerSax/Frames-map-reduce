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
Module      : Frames.Folds.General
Description : Types and functions to simplify folding over Vinyl/Frames records. Leans heavily on the foldl package. 
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Frames.Folds contains various helper functions designed to simplify folding over Frames/Vinyl records given some way of folding over each column.
-}
module Frames.Folds.General
  (
    -- * Types
    EndoFold

    -- ** Types to act as "interpretation functors" for records of folds
  , FoldEndo(..)
  , FoldRecord(..)

  -- * classes
  , EndoFieldFoldsToRecordFolds
  , ConstrainedField

  -- * functions for building records of folds
  , recFieldF
  , fieldToFieldFold

  -- * functions for turning records of folds into folds of records
  , sequenceRecFold
  , sequenceEndoFolds

  -- * functions using constraints to extend an endo-fold across a record
  , foldAll
  , foldAllConstrained
  , functorFoldAllConstrained
  , foldAllMonoid
  )
where

import           Frames.MapReduce.General       ( RecGetFieldC(..)
                                                , RCastC(..)
                                                , IsoRec(..)
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


-- | Turn and EndoFold (Maybe a) into an EndoFold ((Maybe :. ElField) '(s, a))
fieldFold
  :: (Functor f, V.KnownField t)
  => EndoFold (f (V.Snd t))
  -> EndoFold ((f :. ElField) t)
fieldFold =
  P.dimap (fmap (\(V.Field x) -> x) . V.getCompose) (V.Compose . fmap V.Field)
{-# INLINABLE fieldFold #-}

-- | Wrapper for Endo-folds of the field types of ElFields
newtype FoldEndo f t = FoldEndo { unFoldEndo :: EndoFold (f (V.Snd t)) }

-- | Wrapper for folds from a record to an interpreted field.  Usually g ~ ElField
newtype FoldRecord record f g rs a = FoldRecord { unFoldRecord :: FL.Fold (record (f :. ElField) rs) (g a) }

-- | Control.Foldl helper for filtering
filteredFold :: (f a -> Maybe a) -> FL.Fold a b -> FL.Fold (f a) b
filteredFold toMaybe (FL.Fold step begin done) = FL.Fold step' begin done
  where step' x = maybe x (step x) . toMaybe

-- | Helper for building a 'FoldRecord' from a given fold and function of the record
recFieldF
  :: forall t rs a record f
   . (V.KnownField t, Applicative f)
  => (forall x . f x -> Maybe x)
  -> FL.Fold a (V.Snd t) -- ^ A fold from some type a to the field type of an ElField
  -> (record (f :. ElField) rs -> f a)
  -> FoldRecord record f (f :. ElField) rs t -- ^ the resulting 'FoldRecord'-wrapped fold 
recFieldF toMaybe fld fromRecF = FoldRecord
  $ P.dimap fromRecF (V.Compose . pure . V.Field) (filteredFold toMaybe fld)
{-# INLINABLE recFieldF #-}

-- | special case of 'recFieldF' for the case when the function from the record to the folded type
-- is just retrieving the value in a field.
fieldToFieldFold
  :: forall x y rs record f
   . ( V.KnownField x
     , V.KnownField y
     , F.ElemOf rs x
     , RecGetFieldC x record f rs
     , Applicative f
     )
  => (forall z . f z -> Maybe z)
  -> FL.Fold (V.Snd x) (V.Snd y) -- ^ the fold to be wrapped
  -> FoldRecord record f (f :. ElField) rs y -- ^ the wrapped fold
fieldToFieldFold toMaybe fld = recFieldF toMaybe fld (rgetFieldF @x)
{-# INLINABLE fieldToFieldFold #-}

-- | Expand a record of folds, each from the entire record to one field, into a record of folds each from a larger record to the smaller one.
expandFoldInRecord
  :: forall rs as record f
   . (RCastC as rs record f, V.RMap as)
  => V.Rec (FoldRecord record f (f :. ElField) as) as -- ^ original fold 
  -> V.Rec (FoldRecord record f (f :. ElField) rs) as -- ^ resulting fold 
expandFoldInRecord = V.rmap (FoldRecord . FL.premap rcastF . unFoldRecord)
{-# INLINABLE expandFoldInRecord #-}

-- | Change a record of single field folds to a record of folds from the entire record to each field
class EndoFieldFoldsToRecordFolds rs record f where
  endoFieldFoldsToRecordFolds :: F.Rec (FoldFieldEndo (f :. ElField)) rs -> F.Rec (FoldRecord record f (f :. ElField) rs) rs


instance EndoFieldFoldsToRecordFolds '[] record f where
  endoFieldFoldsToRecordFolds _ = V.RNil
  {-# INLINABLE endoFieldFoldsToRecordFolds #-}

instance (EndoFieldFoldsToRecordFolds rs record f
         , RCastC rs (r ': rs) record f
         , V.KnownField r
         , RecGetFieldC r record f (r ': rs)
         , V.RMap rs
         ) => EndoFieldFoldsToRecordFolds (r ': rs) record f where
  endoFieldFoldsToRecordFolds (fe V.:& fes) = FoldRecord (FL.premap (rgetF @r) (unFoldFieldEndo fe)) V.:& expandFoldInRecord @(r ': rs) (endoFieldFoldsToRecordFolds fes)
  {-# INLINABLE endoFieldFoldsToRecordFolds #-}

-- can we do all/some of this via F.Rec (Fold as) bs?
-- | Turn a Record of folds into a fold over records
sequenceRecFold
  :: forall as rs record f
   . (IsoRec rs record f)
  => F.Rec (FoldRecord record f (f :. ElField) as) rs
  -> FL.Fold (record (f :. ElField) as) (record (f :. ElField) rs)
sequenceRecFold = fmap fromRec . V.rtraverse unFoldRecord
{-# INLINABLE sequenceRecFold #-}

-- | turn a record of folds over each field, into a fold over records 
sequenceFieldEndoFolds
  :: (EndoFieldFoldsToRecordFolds rs record f, IsoRec rs record f)
  => F.Rec (FoldFieldEndo (f :. ElField)) rs
  -> FL.Fold (record (f :. ElField) rs) (record (f :. ElField) rs)
sequenceFieldEndoFolds = sequenceRecFold . endoFieldFoldsToRecordFolds
{-# INLINABLE sequenceFieldEndoFolds #-}

liftFold
  :: (V.KnownField t, Functor f)
  => FL.Fold (f (V.Snd t)) (f (V.Snd t))
  -> FoldFieldEndo (f :. ElField) t
liftFold = FoldFieldEndo . fieldFold
{-# INLINABLE liftFold #-}

-- This is not a natural transformation, FoldEndoT ~> FoldEndo F.EField, because of the constraint
liftFoldEndo
  :: (V.KnownField t, Functor f)
  => FoldEndo f t
  -> FoldFieldEndo (f :. ElField) t
liftFoldEndo = FoldFieldEndo . fieldFold . unFoldEndo
{-# INLINABLE liftFoldEndo #-}

liftFolds
  :: (V.RPureConstrained V.KnownField rs, V.RApply rs, Functor f)
  => F.Rec (FoldEndo f) rs
  -> F.Rec (FoldFieldEndo (f :. ElField)) rs
liftFolds = V.rapply liftedFs
  where liftedFs = V.rpureConstrained @V.KnownField $ V.Lift liftFoldEndo
{-# INLINABLE liftFolds #-}

-- | turn a record of endo-folds over each field, into a fold over records 
sequenceEndoFolds
  :: forall rs record f
   . ( V.RApply rs
     , V.RPureConstrained V.KnownField rs
     , EndoFieldFoldsToRecordFolds rs record f
     , IsoRec rs record f
     , Functor f
     )
  => F.Rec (FoldEndo f) rs
  -> FL.Fold (record (f :. ElField) rs) (record (f :. ElField) rs)
sequenceEndoFolds = sequenceFieldEndoFolds . liftFolds
{-# INLINABLE sequenceEndoFolds #-}

-- | apply an unconstrained endo-fold, e.g., a fold which takes the last item in a container, to every field in a record
foldAll
  :: ( V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs record f
     , IsoRec rs record f
     , Functor f
     )
  => (forall a . FL.Fold a a)
  -> FL.Fold (record (f :. ElField) rs) (record (f :. ElField) rs)
foldAll f = sequenceEndoFolds $ V.rpureConstrained @V.KnownField (FoldEndo f)
{-# INLINABLE foldAll #-}

class (c  (V.Snd t)) => ConstrainedField c t
instance (c  (V.Snd t)) => ConstrainedField c t

-- | Apply a constrained endo-fold to all fields of a record.
-- May require a use of TypeApplications, e.g., foldAllConstrained @Num FL.sum
foldAllConstrained
  :: forall c rs record f
   . ( V.RPureConstrained (ConstrainedField c) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs record f
     , IsoRec rs record f
     , Applicative f
     )
  => (forall a . f a -> Maybe a)
  -> (forall a . c a => FL.Fold a a)
  -> FL.Fold (record (f :. ElField) rs) (record (f :. ElField) rs)
foldAllConstrained toMaybe f =
  sequenceEndoFolds $ V.rpureConstrained @(ConstrainedField c)
    (FoldEndo (fmap pure $ filteredFold toMaybe f))
{-# INLINABLE foldAllConstrained #-}

functorFoldAllConstrained
  :: forall c rs record f
   . ( V.RPureConstrained (ConstrainedField c) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs record f
     , IsoRec rs record f
     , Applicative f
     )
  => (forall a . c a => FL.Fold (f a) (f a))
  -> FL.Fold (record (f :. ElField) rs) (record (f :. ElField) rs)
functorFoldAllConstrained f =
  sequenceEndoFolds $ V.rpureConstrained @(ConstrainedField c) (FoldEndo f)
{-# INLINABLE functorFoldAllConstrained #-}

-- | Given a monoid-wrapper, e.g., Sum, apply the derived endo-fold to all fields of a record
-- This is strictly less powerful than foldAllConstrained but might be simpler to use in some cases
foldAllMonoid
  :: forall g rs record f
   . ( V.RPureConstrained (ConstrainedField (MonoidalField g)) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs record f
     , IsoRec rs record f
     , Applicative f
     )
  => (forall a . f a -> Maybe a)
  -> FL.Fold (record (f :. ElField) rs) (record (f :. ElField) rs)
foldAllMonoid toMaybe =
  foldAllConstrained @(MonoidalField g) toMaybe $ monoidWrapperToFold @g
{-# INLINABLE foldAllMonoid #-}

