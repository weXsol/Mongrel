xquery version "3.0";

module namespace eval="http://exist-db.org/mongodb/test/eval";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/xquery/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/ext/mongodb/test/support"
                at "./support.xqm";
                

(: Connect to mongodb, store token :)
declare %test:setUp function eval:setup()
{
    support:setup()
};

(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function eval:cleanup()
{
    support:cleanup()
};


(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : collection#eval(two params)  integer -> double
 :)
declare 
    %test:assertEquals(7)
function eval:eval_numbers() {
    let $mongodbClientId := support:getToken()
    let $eval:= mongodb:eval($mongodbClientId, $support:database, 

    (: Javascript :)
    'function( x, y ) { return x + y; }', 

    (: Parameters :)
    (2, 5)   )
    
    return $eval
};

(: 
 : collection#eval(one parameter) string
 :)
declare 
    %test:assertEquals("test")
function eval:eval_string() {
    let $mongodbClientId := support:getToken()
    let $eval:= mongodb:eval($mongodbClientId, $support:database, 

    (: Javascript :)
    'function( value ) { return value; }', 

    (: Parameters :)
    ("test")   )
    
    return $eval
};

(: 
 : collection#eval() 
 : <json type="object">
 : <pair name="x" type="number">1.0</pair>
 : <pair name="y" type="number">2.0</pair>
 : </json>
 :)
declare 
    %test:assertEquals(2)
function eval:eval_json() {
    let $mongodbClientId := support:getToken()
    let $eval:= mongodb:eval($mongodbClientId, $support:database, 

    (: Javascript :)
    'function() { return { x : 1 ,  y : 2 } }' )
    
    return count(xqjson:parse-json($eval)//pair)
};
