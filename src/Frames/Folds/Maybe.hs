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
Module      : Frames.Folds
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
  -- , foldAllMonoid
  )
where

import           Frames.MapReduce.Maybe         ( rgetMaybeField )
import           Frames.Folds                   ( EndoFold
                                                , FoldFieldEndo(..)
                                                , monoidWrapperToFold
                                                , MonoidalField
                                                )

import qualified Control.Foldl                 as FL
import qualified Control.Newtype               as N
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
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
  :: (V.KnownField t, a ~ V.Snd t)
  => EndoFold (Maybe a)
  -> EndoFold ((Maybe :. ElField) t)
fieldFold =
  P.dimap (fmap (\(V.Field x) -> x) . V.getCompose) (V.Compose . fmap V.Field)
{-# INLINABLE fieldFold #-}

-- | Wrapper for Endo-folds of the field types of ElFields
newtype FoldEndo t = FoldEndo { unFoldEndo :: EndoFold (Maybe (V.Snd t)) }

-- | Wrapper for folds from a record to an interpreted field.  Usually f ~ ElField
newtype FoldRecord f rs a = FoldRecord { unFoldRecord :: FL.Fold (F.Rec (Maybe :. ElField) rs) (f a) }

-- | Control.Foldl helper for filtering Nothings
maybeFold :: FL.Fold a b -> FL.Fold (Maybe a) b
maybeFold (FL.Fold step begin done) = FL.Fold step' begin done
  where step' x ma = maybe x (step x) ma

-- | Helper for building a 'FoldRecord' from a given fold and function of the record
recFieldF
  :: forall t rs a
   . V.KnownField t
  => FL.Fold a (V.Snd t) -- ^ A fold from some type a to the field type of an ElField 
  -> (F.Rec (Maybe :. ElField) rs -> Maybe a) -- ^ a function to get the a value from the input record
  -> FoldRecord (Maybe :. ElField) rs t -- ^ the resulting 'FoldRecord'-wrapped fold 
recFieldF fld fromRec =
  FoldRecord $ P.dimap fromRec (V.Compose . Just . V.Field) (maybeFold fld)
{-# INLINABLE recFieldF #-}

-- | special case of 'recFieldF' for the case when the function from the record to the folded type
-- is just retrieving the value in a field.
fieldToFieldFold
  :: forall x y rs
   . (V.KnownField x, V.KnownField y, F.ElemOf rs x)
  => FL.Fold (V.Snd x) (V.Snd y) -- ^ the fold to be wrapped
  -> FoldRecord (Maybe :. ElField) rs y -- ^ the wrapped fold
fieldToFieldFold fld = recFieldF fld (rgetMaybeField @x)
{-# INLINABLE fieldToFieldFold #-}

-- | Expand a record of folds, each from the entire record to one field, into a record of folds each from a larger record to the smaller one.
expandFoldInRecord
  :: forall rs as
   . (as F.⊆ rs, V.RMap as)
  => F.Rec (FoldRecord (Maybe :. ElField) as) as -- ^ original fold 
  -> F.Rec (FoldRecord (Maybe :. ElField) rs) as -- ^ resulting fold 
expandFoldInRecord = V.rmap (FoldRecord . FL.premap F.rcast . unFoldRecord)
{-# INLINABLE expandFoldInRecord #-}

-- | Change a record of single field folds to a record of folds from the entire record to each field
class EndoFieldFoldsToRecordFolds rs where
  endoFieldFoldsToRecordFolds :: F.Rec (FoldFieldEndo (Maybe :. ElField)) rs -> F.Rec (FoldRecord (Maybe :. ElField) rs) rs


instance EndoFieldFoldsToRecordFolds '[] where
  endoFieldFoldsToRecordFolds _ = V.RNil
  {-# INLINABLE endoFieldFoldsToRecordFolds #-}

instance (EndoFieldFoldsToRecordFolds rs, rs F.⊆ (r ': rs), V.RMap rs) => EndoFieldFoldsToRecordFolds (r ': rs) where
  endoFieldFoldsToRecordFolds (fe V.:& fes) = FoldRecord (FL.premap (V.rget @r) (unFoldFieldEndo fe)) V.:& expandFoldInRecord @(r ': rs) (endoFieldFoldsToRecordFolds fes)
  {-# INLINABLE endoFieldFoldsToRecordFolds #-}

-- can we do all/some of this via F.Rec (Fold as) bs?
-- | Turn a Record of folds into a fold over records
sequenceRecFold
  :: forall as rs
   . F.Rec (FoldRecord (Maybe :. ElField) as) rs
  -> FL.Fold (F.Rec (Maybe :. ElField) as) (F.Rec (Maybe :. ElField) rs)
sequenceRecFold = V.rtraverse unFoldRecord
{-# INLINABLE sequenceRecFold #-}

-- | turn a record of folds over each field, into a fold over records 
sequenceFieldEndoFolds
  :: EndoFieldFoldsToRecordFolds rs
  => F.Rec (FoldFieldEndo (Maybe :. ElField)) rs
  -> FL.Fold (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
sequenceFieldEndoFolds = sequenceRecFold . endoFieldFoldsToRecordFolds
{-# INLINABLE sequenceFieldEndoFolds #-}

liftFold
  :: V.KnownField t
  => FL.Fold (Maybe (V.Snd t)) (Maybe (V.Snd t))
  -> FoldFieldEndo (Maybe :. ElField) t
liftFold = FoldFieldEndo . fieldFold
{-# INLINABLE liftFold #-}

-- This is not a natural transformation, FoldEndoT ~> FoldEndo F.EField, because of the constraint
liftFoldEndo
  :: V.KnownField t => FoldEndo t -> FoldFieldEndo (Maybe :. ElField) t
liftFoldEndo = FoldFieldEndo . fieldFold . unFoldEndo
{-# INLINABLE liftFoldEndo #-}

liftFolds
  :: (V.RPureConstrained V.KnownField rs, V.RApply rs)
  => F.Rec FoldEndo rs
  -> F.Rec (FoldFieldEndo (Maybe :. ElField)) rs
liftFolds = V.rapply liftedFs
  where liftedFs = V.rpureConstrained @V.KnownField $ V.Lift liftFoldEndo
{-# INLINABLE liftFolds #-}


-- | turn a record of endo-folds over each field, into a fold over records 
sequenceEndoFolds
  :: forall rs
   . ( V.RApply rs
     , V.RPureConstrained V.KnownField rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => F.Rec FoldEndo rs
  -> FL.Fold (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
sequenceEndoFolds = sequenceFieldEndoFolds . liftFolds
{-# INLINABLE sequenceEndoFolds #-}

-- | apply an unconstrained endo-fold, e.g., a fold which takes the last item in a container, to every field in a record
foldAll
  :: ( V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => (forall a . FL.Fold a a)
  -> FL.Fold (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
foldAll f = sequenceEndoFolds $ V.rpureConstrained @V.KnownField (FoldEndo f)
{-# INLINABLE foldAll #-}

class (c  (V.Snd t)) => ConstrainedField c t
instance (c  (V.Snd t)) => ConstrainedField c t

-- | Apply a constrained endo-fold to all fields of a record.
-- May require a use of TypeApplications, e.g., foldAllConstrained @Num FL.sum
foldAllConstrained
  :: forall c rs
   . ( V.RPureConstrained (ConstrainedField c) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => (forall a . c a => FL.Fold a a)
  -> FL.Fold (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
foldAllConstrained f =
  sequenceEndoFolds $ V.rpureConstrained @(ConstrainedField c)
    (FoldEndo (fmap Just $ maybeFold f))
{-# INLINABLE foldAllConstrained #-}

maybeFoldAllConstrained
  :: forall c rs
   . ( V.RPureConstrained (ConstrainedField c) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => (forall a . c a => FL.Fold (Maybe a) (Maybe a))
  -> FL.Fold (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
maybeFoldAllConstrained f =
  sequenceEndoFolds $ V.rpureConstrained @(ConstrainedField c) (FoldEndo f)
{-# INLINABLE maybeFoldAllConstrained #-}

-- | Given a monoid-wrapper, e.g., Sum, apply the derived endo-fold to all fields of a record
-- This is strictly less powerful than foldAllConstrained but might be simpler to use in some cases
foldAllMonoid
  :: forall f rs
   . ( V.RPureConstrained (ConstrainedField (MonoidalField f)) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => FL.Fold (F.Rec (Maybe :. ElField) rs) (F.Rec (Maybe :. ElField) rs)
foldAllMonoid = foldAllConstrained @(MonoidalField f) $ monoidWrapperToFold @f
{-# INLINABLE foldAllMonoid #-}
