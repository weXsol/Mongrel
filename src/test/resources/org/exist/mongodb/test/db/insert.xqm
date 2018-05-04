xquery version "3.1";

module namespace mongoMain = "http://exist-db.org/mongodb/test/insert";


import module namespace mongodb = "http://expath.org/ns/mongo";
import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
import module namespace test = "http://exist-db.org/xquery/xqsuite"
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";


(: Connect to mongodb, store token :)
declare %test:setUp function mongoMain:setup()
{
    support:setup()
};

(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function mongoMain:cleanup()
{
    support:cleanup()
};

(: 
 : Actual tests below this line  
 :)

(: collection#insert()  insert one document :)
declare 
    %test:assertEquals(1)
function mongoMain:insert() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   "{ x : 1  ,  y : 2 , z : 3 }")
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 1 }")
};

(: collection#insert()  insert two documents :)
declare 
    %test:assertEquals(2)
function mongoMain:insert_two() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   ("{ x : 2  ,  y : 1 , z : 3 }", "{ x : 2  ,  y : 2 , z : 3 }"))
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 2 }")
};

(: collection#insert()  empty sequence :)
declare 
    %test:assertError("mongodb:json")
function mongoMain:insert_emptysequence() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   () )
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 10 }")
};

(: collection#insert()  empty document :)
declare 
    %test:assertError("mongodb:json")
function mongoMain:insert_emptydocument() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   "" )
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 10 }")
};

(: collection#insert()  insert one document :)
declare 
    %test:assertEquals(1)
function mongoMain:insert_xq31() {
    let $mongodbClientId := support:getToken()
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $data := parse-json("{ x : 100  ,  y : 200 , z : 300 }", $options)
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, $data )
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 100 }")
};

