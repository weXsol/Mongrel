xquery version "3.0";

module namespace mongoMain="http://exist-db.org/ext/mongodb/test/login";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/xquery/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

declare variable $mongoMain:mongoUrl := "mongodb://miniserver.local";
declare variable $mongoMain:database := "mydatabase";
declare variable $mongoMain:testCollection := "/db/mongodbTest";
declare variable $mongoMain:tokenStore := "token.xml";

(: 
 : Make sure no mongodb connections are active.
 :)
declare 
    %test:setUp
    %test:tearDown
function mongoMain:setup() {
    for $mongodbClientId in mongodb:list-mongodb-clientids()
    return
        mongodb:close($mongodbClientId)
};

(: 
 : Simple login-logout test.
 :)
declare 
    %test:assertEquals(1)
function mongoMain:simple_connect_close() {
    let $mongodbClientId := mongodb:connect($mongoMain:mongoUrl)
    let $count := count(mongodb:list-mongodb-clientids())
    let $logout := mongodb:close($mongodbClientId)
    return $count
};

(:
 : Perform multiple logins, make sure tokens are unique
 : and make sure all clients are closed correctly.
 :)
declare 
    %test:arg("n", 100) %test:assertEquals(100, 100, 100, 0) 
function mongoMain:multi_connect_close($n as xs:int)
{
    let $mongodbClientIds := for $i in (1 to $n) return mongodb:connect($mongoMain:mongoUrl)
    let $distinctTokens := distinct-values($mongodbClientIds)
    let $preActiveClientIds := mongodb:list-mongodb-clientids()
    let $nop := for $mongodbClientId in $preActiveClientIds return mongodb:close($mongodbClientId)
    let $postActiveClientIds := mongodb:list-mongodb-clientids()
    return
        (count($mongodbClientIds), count($distinctTokens), count($preActiveClientIds), count($postActiveClientIds))
};


(:
 : Simple test with bogus token.
 :)
declare %test:assertError("mongodb:MONG0001") function mongoMain:connect_invalid_token()
{
    mongodb:list-collections($mongoMain:database, $mongoMain:database)
};


