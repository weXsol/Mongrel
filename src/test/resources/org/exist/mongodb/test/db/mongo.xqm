xquery version "3.0";

module namespace mongoMain = "http://exist-db.org/mongodb/test/main";

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
declare 
    %test:assertError("mongodb:id")
function mongoMain:login_illegal_token() {
    let $mongodbClientId := "aa"
    return mongodb:drop($mongodbClientId, $support:database, $support:mongoCollection)
};

declare
    %test:assertEquals(3)
function mongoMain:count_clients() {
     let $count1 := count( mongodb:list-mongodb-clientids() )
     let $token1 := mongodb:connect("mongodb://localhost")
     let $count2 := count(mongodb:list-mongodb-clientids())
     let $token2 := mongodb:connect("mongodb://localhost")
     let $token3 := mongodb:connect("mongodb://localhost")
     let $count3 := count(mongodb:list-mongodb-clientids())

     return ($count3 - $count1)
};