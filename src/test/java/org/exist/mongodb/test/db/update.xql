xquery version "3.1";

module namespace mongoMain="http://exist-db.org/mongodb/test/update";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

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

(: collection#update() add contents  :)
declare 
    %test:assertEquals(1,1)
function mongoMain:update_overwrite() {
    let $mongodbClientId := support:getToken()
    let $insert := mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 1  ,  y : 2 , z : 3 }")
                     
    let $update := mongodb:update($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 1 }", "{'$set' : { q : 4 }}")            

    return
        (
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 1 }"),
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ q : 4 }")
        )
};


(: collection#update() noupsert contents :)
declare 
    %test:assertEquals(0)
function mongoMain:update_noupsert() {
    let $mongodbClientId := support:getToken()
                     
    let $update := mongodb:update($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 2 }", "{ qa : 4 }", false(), false())            

    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ qa : 4 }")
};

declare 
    %test:assertEquals(0)
function mongoMain:update_noupsert_xq31() {
    let $mongodbClientId := support:getToken()
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $criterium := parse-json("{ x : 4 }", $options)
    let $modification := parse-json("{ qa : 6 }", $options)
                     
    let $update := mongodb:update($mongodbClientId, $support:database, $support:mongoCollection, 
                     $criterium, $modification, false(), false())            

    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ qa : 6 }")
};


(: collection#update() upsert contents :)
declare 
    %test:assertEquals(1)
function mongoMain:update_upsert() {
    let $mongodbClientId := support:getToken()
                     
    let $update := mongodb:update($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 3 }", "{ qb : 4 }", true(), false())            

    return
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ qb : 4 }")
};


(: collection#update() multi, 2 documents  :)
declare 
    %test:assertEquals(2,2)
function mongoMain:update_multi() {
    let $mongodbClientId := support:getToken()
    let $insert1 := mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 10  ,  y : 1 , z : 3 }")
    let $insert2 := mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 10  ,  y : 2 , z : 3 }")
                     
    let $update := mongodb:update($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 10 }", "{'$set' : { qc : 4 }}", false(), true())            

    return
        (
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 10 }"),
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ qc : 4 }")
        )
};

(: collection#update() nomulti, 2 documents  :)
declare 
    %test:assertEquals(2,1)
function mongoMain:update_nomulti() {
    let $mongodbClientId := support:getToken()
    let $insert1 := mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 20  ,  y : 1 , z : 3 }")
    let $insert2 := mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 20  ,  y : 2 , z : 3 }")
                     
    let $update := mongodb:update($mongodbClientId, $support:database, $support:mongoCollection, 
                     "{ x : 20 }", "{'$set' : { qd : 4 }}", false(), false())            

    return
        (
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ x : 20 }"),
        mongodb:count($mongodbClientId, $support:database, $support:mongoCollection, "{ qd : 4 }")
        )
};



