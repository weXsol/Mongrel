xquery version "3.1";

module namespace json="http://exist-db.org/mongodb/test/json";

declare namespace output="http://www.w3.org/2010/xslt-xquery-serialization";


import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace bson = "http://exist-db.org/xquery/mongodb/bson" 
                at "java:org.exist.mongodb.xquery.BSonModule";

declare variable $json:serializeOptions := <output:serialization-parameters>
                          <output:method>json</output:method>
                          <output:indent>false</output:indent>
                      </output:serialization-parameters>;

(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)



declare 
    %test:assertEquals('{"x":"1","y":"2","z":"3"}')
function json:larger_json() {
     let $data := bson:parse("{ x : 1 , y : 2 , z : 3 }")
     return
         serialize($data, $json:serializeOptions)
};

