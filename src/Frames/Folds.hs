{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Frames.Monomorphic.Folds
Description : Types and functions to simplify folding over Vinyl/Frames records. Leans heavily on the foldl package. 
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Frames.Folds contains various helper functions designed to simplify folding over Frames/Vinyl records given some way of folding over each column.
-}
module Frames.Folds
  (
    -- * Types
    EndoFold

    -- ** Types to act as "interpretation functors" for records of folds
  , FoldEndo(..)
  , FoldFieldEndo(..)
  , FoldRecord(..)

  -- * functions for building records of folds
  , toFoldRecord
  , recFieldF
  , fieldToFieldFold

  -- * functions for turning records of folds into folds of records
  , sequenceRecFold
  , sequenceEndoFolds

  -- * functions using constraints to extend an endo-fold across a record
  , foldAll
  , foldAllConstrained
  , foldAllMonoid

  -- * for generalizing
  , monoidWrapperToFold
  , MonoidalField
  )
where

import qualified Control.Foldl                 as FL
import qualified Control.Newtype               as N
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Profunctor               as P
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import qualified Frames.Melt                   as F


-- | A Type synonym for folds like sum or, often, average.
type EndoFold a = FL.Fold a a

-- | Turn and EndoFold a into an EndoFold (ElField '(s, a))
fieldFold
  :: (V.KnownField t, a ~ V.Snd t) => EndoFold a -> EndoFold (F.ElField t)
fieldFold = P.dimap (\(V.Field x) -> x) V.Field
{-# INLINABLE fieldFold #-}

-- | Wrapper for Endo-folds of the field types of ElFields
newtype FoldEndo t = FoldEndo { unFoldEndo :: EndoFold (V.Snd t) }

-- | Wrapper for endo-folds on an interpretation f.  Usually f ~ ElField 
newtype FoldFieldEndo f a = FoldFieldEndo { unFoldFieldEndo :: EndoFold (f a) } -- type FoldFieldEndo f a = FoldEndo (f a)

-- | Wrapper for folds from a record to an interpreted field.  Usually f ~ ElField
newtype FoldRecord f rs a = FoldRecord { unFoldRecord :: FL.Fold (F.Record rs) (f a) }

-- | Create a @FoldRecord@ from a @Fold@ from a record to a specific type.
-- This is helpful when creating folds from a record to another record (or the same record)
-- by building it one field at a time.  See examples for details.
toFoldRecord
  :: V.KnownField t
  => FL.Fold (F.Record rs) (V.Snd t)
  -> FoldRecord F.ElField rs t
toFoldRecord = FoldRecord . fmap V.Field
{-# INLINABLE toFoldRecord #-}

-- | Helper for building a 'FoldRecord' from a given fold and function of the record
recFieldF
  :: forall t rs a
   . V.KnownField t
  => FL.Fold a (V.Snd t) -- ^ A fold from some type a to the field type of an ElField 
  -> (F.Record rs -> a) -- ^ a function to get the a value from the input record
  -> FoldRecord V.ElField rs t -- ^ the resulting 'FoldRecord'-wrapped fold 
recFieldF fld fromRec = FoldRecord $ P.dimap fromRec V.Field fld
{-# INLINABLE recFieldF #-}

-- | special case of 'recFieldF' for the case when the function from the record to the folded type
-- is just retrieving the value in a field.
fieldToFieldFold
  :: forall x y rs
   . (V.KnownField x, V.KnownField y, F.ElemOf rs x)
  => FL.Fold (V.Snd x) (V.Snd y) -- ^ the fold to be wrapped
  -> FoldRecord F.ElField rs y -- ^ the wrapped fold
fieldToFieldFold fld = recFieldF fld (F.rgetField @x)
{-# INLINABLE fieldToFieldFold #-}

-- | Expand a record of folds, each from the entire record to one field, into a record of folds each from a larger record to the smaller one.
expandFoldInRecord
  :: forall rs as
   . (as F.⊆ rs, V.RMap as)
  => F.Rec (FoldRecord F.ElField as) as -- ^ original fold 
  -> F.Rec (FoldRecord F.ElField rs) as -- ^ resulting fold 
expandFoldInRecord = V.rmap (FoldRecord . FL.premap F.rcast . unFoldRecord)
{-# INLINABLE expandFoldInRecord #-}

-- | Change a record of single field folds to a record of folds from the entire record to each field
class EndoFieldFoldsToRecordFolds rs where
  endoFieldFoldsToRecordFolds :: F.Rec (FoldFieldEndo F.ElField) rs -> F.Rec (FoldRecord F.ElField rs) rs


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
   . F.Rec (FoldRecord F.ElField as) rs
  -> FL.Fold (F.Record as) (F.Record rs)
sequenceRecFold = V.rtraverse unFoldRecord
{-# INLINABLE sequenceRecFold #-}

-- | turn a record of folds over each field, into a fold over records 
sequenceFieldEndoFolds
  :: EndoFieldFoldsToRecordFolds rs
  => F.Rec (FoldFieldEndo F.ElField) rs
  -> FL.Fold (F.Record rs) (F.Record rs)
sequenceFieldEndoFolds = sequenceRecFold . endoFieldFoldsToRecordFolds
{-# INLINABLE sequenceFieldEndoFolds #-}

{-
liftFold
  :: V.KnownField t => FL.Fold (V.Snd t) (V.Snd t) -> FoldFieldEndo F.ElField t
liftFold = FoldFieldEndo . fieldFold
{-# INLINABLE liftFold #-}
-}

-- This is not a natural transformation, FoldEndoT ~> FoldEndo F.EField, because of the constraint
liftFoldEndo :: V.KnownField t => FoldEndo t -> FoldFieldEndo F.ElField t
liftFoldEndo = FoldFieldEndo . fieldFold . unFoldEndo
{-# INLINABLE liftFoldEndo #-}

liftFolds
  :: (V.RPureConstrained V.KnownField rs, V.RApply rs)
  => F.Rec FoldEndo rs
  -> F.Rec (FoldFieldEndo F.ElField) rs
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
  -> FL.Fold (F.Record rs) (F.Record rs)
sequenceEndoFolds = sequenceFieldEndoFolds . liftFolds
{-# INLINABLE sequenceEndoFolds #-}

-- | apply an unconstrained endo-fold, e.g., a fold which takes the last item in a container, to every field in a record
foldAll
  :: ( V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => (forall a . FL.Fold a a)
  -> FL.Fold (F.Record rs) (F.Record rs)
foldAll f = sequenceEndoFolds $ V.rpureConstrained @V.KnownField (FoldEndo f)
{-# INLINABLE foldAll #-}

class (c (V.Snd t)) => ConstrainedField c t
instance (c (V.Snd t)) => ConstrainedField c t

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
  -> FL.Fold (F.Record rs) (F.Record rs)
foldAllConstrained f =
  sequenceEndoFolds $ V.rpureConstrained @(ConstrainedField c) (FoldEndo f)
{-# INLINABLE foldAllConstrained #-}

-- | Given a monoid-wrapper, e.g., Sum, and functions to wrap and unwrap, we can produce an endo-fold on a
monoidWrapperToFold
  :: forall f a . (N.Newtype (f a) a, Monoid (f a)) => FL.Fold a a
monoidWrapperToFold = FL.Fold (\w a -> N.pack a <> w) (mempty @(f a)) N.unpack -- is this the correct order in (<>) ?
{-# INLINABLE monoidWrapperToFold #-}

class (N.Newtype (f a) a, Monoid (f a)) => MonoidalField f a
instance (N.Newtype (f a) a, Monoid (f a)) => MonoidalField f a

-- | Given a monoid-wrapper, e.g., Sum, apply the derived endo-fold to all fields of a record
-- This is strictly less powerful than foldAllConstrained but might be simpler to use in some cases
foldAllMonoid
  :: forall f rs
   . ( V.RPureConstrained (ConstrainedField (MonoidalField f)) rs
     , V.RPureConstrained V.KnownField rs
     , V.RApply rs
     , EndoFieldFoldsToRecordFolds rs
     )
  => FL.Fold (F.Record rs) (F.Record rs)
foldAllMonoid = foldAllConstrained @(MonoidalField f) $ monoidWrapperToFold @f
{-# INLINABLE foldAllMonoid #-}
