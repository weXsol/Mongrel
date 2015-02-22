xquery version "3.0";

module namespace mongoMain="http://exist-db.org/mongodb/test/insert";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/mongrel/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/ext/mongodb/test/support"
                at "./support.xqm";
                

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
        mongodb:count-documents($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 1 }")
};

(: collection#insert()  insert two documents :)
declare 
    %test:assertEquals(2)
function mongoMain:insert_two() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   ("{ x : 2  ,  y : 1 , z : 3 }", "{ x : 2  ,  y : 2 , z : 3 }"))
    return
        mongodb:count-documents($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 2 }")
};

(: collection#insert()  empty sequence :)
declare 
    %test:assertEquals(0)
function mongoMain:insert_emptysequence() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   () )
    return
        mongodb:count-documents($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 10 }")
};

(: collection#insert()  empty document :)
declare 
    %test:assertEquals(0)
function mongoMain:insert_emptydocument() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   "" )
    return
        mongodb:count-documents($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 10 }")
};


