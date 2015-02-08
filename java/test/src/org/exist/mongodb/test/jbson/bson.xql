xquery version "3.1";

module namespace json="http://exist-db.org/mongodb/test/json";


import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace bson = "http://exist-db.org/xquery/mongodb/bson" 
                at "java:org.exist.mongodb.xquery.BSonModule";


(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : Parser test using a string with simple value
 :)
declare 
    %test:assertEquals('{ "x" : 1}')
function json:simple_bson() {
    serialize( bson:parse("{ x : 1 }") )
};

