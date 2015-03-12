xquery version "3.1";

module namespace parse="http://exist-db.org/mongodb/test/parse";

declare namespace output="http://www.w3.org/2010/xslt-xquery-serialization";


import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace bson = "http://exist-db.org/mongrel/bson" 
                at "java:org.exist.mongodb.xquery.BSonModule";

declare variable $parse:serializeOptions := <output:serialization-parameters>
                          <output:method>json</output:method>
                          <output:indent>false</output:indent>
                      </output:serialization-parameters>;

(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : Parser test using a string with simple value
 :)

declare 
    %test:assertEquals('{"x":1}')
function parse:simple_json() {
     let $data := bson:pa rse("{ x : 1 }")
     return
         serialize($data,$parse:serializeOptions)
};

declare 
    %test:assertEquals('{"x":1,"y":2,"z":3}')
function parse:larger_json() {
     let $data := bson:parse("{ x : 1 , y : 2 , z : 3 }")
     return
         serialize($data, $parse:serializeOptions)
};

declare 
    %test:assertEquals('{"name":{"first":"John","last":"Backus"},"awards":[{"by":"IEEE Computer Society","year":1967,"award":"W.W. McDowell Award"},{"by":"National Academy of Engineering","year":1993,"award":"Draper Prize"}],"_id":1,"contribs":["Fortran","ALGOL","Backus-Naur Form","FP"]}')
function parse:complex_json() {
     let $data := bson:parse('{
"_id" : 1,
"name" : { "first" : "John", "last" : "Backus" },
"contribs" : [ "Fortran", "ALGOL", "Backus-Naur Form", "FP" ],
"awards" : [
           {
             "award" : "W.W. McDowell Award",
             "year" : 1967,
             "by" : "IEEE Computer Society"
           },
           { "award" : "Draper Prize",
             "year" : 1993,
             "by" : "National Academy of Engineering"
           }
]
}
')
     return
         serialize($data, $parse:serializeOptions)
};


