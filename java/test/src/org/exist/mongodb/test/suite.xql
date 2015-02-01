xquery version "3.0";

import module namespace test="http://exist-db.org/xquery/xqsuite"
at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

test:suite((
    inspect:module-functions(xs:anyURI("aggregate.xql")),
    inspect:module-functions(xs:anyURI("eval.xql")),
    inspect:module-functions(xs:anyURI("find.xql")),
    inspect:module-functions(xs:anyURI("findAndModify.xql")),
    inspect:module-functions(xs:anyURI("findAndRemove.xql")),
    inspect:module-functions(xs:anyURI("findOne.xql")),
    inspect:module-functions(xs:anyURI("group.xql")),
    inspect:module-functions(xs:anyURI("insert.xql")),
    inspect:module-functions(xs:anyURI("map-reduce.xql")),
    inspect:module-functions(xs:anyURI("mongo.xql")),
    inspect:module-functions(xs:anyURI("save.xql"))
))
