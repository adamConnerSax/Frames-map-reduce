v 0.3.0.0
* Added Combinator for aggregation in Frames.Aggregation along with helpers to create folds over data cols, promote simple functions to
record functions to be used in aggregations, combine key aggregations.
* Added ```toRecordFold``` to the Folds modules to simplify making record to record folds from record to Type folds. See examples.
* Added an example of its use in the example.

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
