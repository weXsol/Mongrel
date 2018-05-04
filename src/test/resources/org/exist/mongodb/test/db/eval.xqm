xquery version "3.1";

module namespace eval = "http://exist-db.org/mongodb/test/eval";


declare namespace test = "http://exist-db.org/xquery/xqsuite";

import module namespace mongodb = "http://expath.org/ns/mongo";

import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
                

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


declare 
    %test:assertEquals(1,2)
function eval:eval_json() {
    let $mongodbClientId := support:getToken()
    let $eval:= mongodb:eval($mongodbClientId, $support:database, 

    (: Javascript :)
    'function() { return { x : 1 ,  y : 2 } }' )
    
    return ($eval?x, $eval?y)
};
