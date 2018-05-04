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