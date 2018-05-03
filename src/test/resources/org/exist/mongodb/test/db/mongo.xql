xquery version "3.0";

module namespace mongoMain="http://exist-db.org/mongodb/test/main";


declare namespace test="http://exist-db.org/org.exist.mongodb.test/xqsuite";

import module namespace mongodb = "http://expath.org/ns/mongo" 
                at "java:org.exist.mongodb.org.exist.mongodb.test.MongodbModule";

import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";

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
    %test:assertEquals("<test n='4'/>")
function mongoMain:show_token() {
    let $token := support:getToken()
    return <test n='4'/>
};

declare 
    %test:assertError("mongodb:id")
function mongoMain:login_illegal_token() {
    let $mongodbClientId := "aa"
    return mongodb:drop($mongodbClientId, $support:database, $support:mongoCollection)
};