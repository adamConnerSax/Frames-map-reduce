v0.4.4.2
* Bumped upper bounds in cabal

v 0.4.1.1
* Bumped some upper bounds

v 0.4.0.0
* Added ```splitOnData``` for assigning via a given set of data cols.
* Bumped upper bounds
* Checked compilation w/ghc-8.10.2
* Fixed a redundant include warning for Monoid using CPP in Frames.Folds

v 0.3.0.0
* Added Combinator for aggregation in Frames.Aggregation along with helpers to create folds over data cols, promote simple functions to
record functions to be used in aggregations, combine key aggregations.
* Added ```toRecordFold``` to the Folds modules to simplify making record to record folds from record to Type folds. See examples.
* Added an example of its use in the example.
* Added the full record/functor generalization in Frames.Aggregation.General
* Added the specialization to ```Maybe``` in Frames.Aggregation.Maybe
* Cleanup of unneccesary imports
* Suppressed Orphan Instance warnings about Hashable instances for ```Record``` and general record types.  These
instances should properly be in Vinyl.
* Bumped some upper bounds for GHC 8.8.  Still can't compile with 8.8+ until discrimination is updated.

v 0.2.0.0
* Added Combinators for ```record (Maybe :. ElField) rs``` (Much thanks to Tim Pierson, @o1lo01ol1o, for the idea and the work!).
* Added Combinators polymorphic in record type (```Rec```, ```ARec``` or ```SRec``` are supported) and composed interpretation functor ```f :. ElField```
* ```Maybe :. ElField``` case now uses the general case in order to avoid code duplication
* ```Record``` case still uses separate code because the amount of new coercion code required was enough to not seem worth the re-use of the polymorphic case.

v 0.1.0.2
* Changed example from "executable" to "test-suite".  Now it doens't need to be built and its dependencies don't leak to library users.

v 0.1.0.1
* lowered containers lower bound
* rasied some outdated upper bounds (profunctors, hashable)

v 0.1.0.0
* Initial hackage release
