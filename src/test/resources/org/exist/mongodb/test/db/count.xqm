xquery version "3.1";

module namespace mongoMain = "http://exist-db.org/mongodb/test/count";


import module namespace mongodb = "http://expath.org/ns/mongo";
import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
import module namespace test = "http://exist-db.org/xquery/xqsuite";
                

(: Connect to mongodb, store token :)
declare %test:setUp function mongoMain:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    let $result := for $i in (1 to 10) 
                   return  
                       ( mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   "{ " || 
                    "x : " || $i || ", " ||
                    "y : " || $i || ", " ||
                    "z : " || ($i * $i) || 
                   "}"),
                   
                   mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   "{ " || 
                    "x : " || $i || ", " ||
                    "y : " || (10 - $i) || ", " ||
                    "z : " || ($i * $i) || 
                   "}")
                       )
                   
    return""
};

(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function mongoMain:cleanup()
{
    support:cleanup()
};

(: 
 : Actual tests below this line  
 :)

(: collection#count() :)
declare 
    %test:assertEquals(20)
function mongoMain:count() {
    let $mongodbClientId := support:getToken()
    return mongodb:count($mongodbClientId, $support:database, $support:mongoCollection)
};

(: collection#count() :)
declare 
    %test:assertEquals(2)
function mongoMain:count_params_xq31() {
    let $mongodbClientId := support:getToken()
    
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $query := parse-json("{ x : 2 }", $options)
    
    return mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, $query)
};



