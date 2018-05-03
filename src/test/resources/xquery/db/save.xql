xquery version "3.0";

module namespace mongoMain="http://exist-db.org/mongodb/test/save";


import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://expath.org/ns/mongo" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/mongrel/test/support"
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

(: collection#save() save three documents  :)
declare 
    %test:assertEquals(3)
function mongoMain:save() {
    let $mongodbClientId := support:getToken()
    let $result :=  (
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 1  ,  y : 2 , z : 3 }"),
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 1  ,  y : 2 , z : 3 }"),
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 1  ,  y : 2 , z : 3 }")             
    )
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 1 }")
};

(: collection#save() save three time the same document  :)
declare 
    %test:assertEquals(1)
function mongoMain:save_samedoc() {
    let $mongodbClientId := support:getToken()
    let $result :=  (
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ '_id' : '1', x : 2  ,  y : 2 , z : 3 }"),
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ '_id' : '1', x : 2  ,  y : 2 , z : 3 }"),
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ '_id' : '1', x : 2  ,  y : 2 , z : 3 }")             
    )
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 2 }")
};

(: collection#save() mix  :)
declare 
    %test:assertEquals(2)
function mongoMain:save_mixed() {
    let $mongodbClientId := support:getToken()
    let $result :=  (
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ '_id' : '1', x : 3  ,  y : 2 , z : 3 }"),
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ '_id' : '1', x : 3  ,  y : 2 , z : 3 }"),
        mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 3  ,  y : 2 , z : 3 }")             
    )
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 3 }")
};



(: collection#save()  empty document :)
declare 
    %test:assertError("mongodb:json")
function mongoMain:save_emptydocument() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                   "" )
    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 10 }")
};


(: collection#save()  raw output 
 : { "serverUsed" : "miniserver.local:27017" , "ok" : 1 , "n" : 1 , "updatedExisting" : true}
 : <json type="object">
 : <pair name="serverUsed" type="string">miniserver.local:27017</pair>
 : <pair name="ok" type="number">1</pair>
 : <pair name="n" type="number">1</pair>
 : <pair name="updatedExisting" type="boolean">true</pair>
 : </json>
 : :)
(: )declare
    %test:assertEquals(4)
function mongoMain:save_raw() {
    let $mongodbClientId := support:getToken()
    let $result :=  mongodb:save($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ '_id' : '1', x : 3  ,  y : 2 , z : 3 }")
    return count( xqjson:parse-json($result)//pair )
};
:)