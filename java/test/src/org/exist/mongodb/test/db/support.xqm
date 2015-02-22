xquery version "3.1";

module namespace support="http://exist-db.org/mongrel/test/support";

import module namespace mongodb = "http://exist-db.org/mongrel/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

declare variable $support:mongoUrl := "mongodb://localhost";
declare variable $support:database := "mydatabase";
declare variable $support:testCollection := "/db/mongodbTest";
declare variable $support:mongoCollection := "mongodbTest";
declare variable $support:tokenStore := "token.xml";

(:  
 : Connect to mongodb, store token 
 :)
declare function support:setup() {
    let $foo :=  xmldb:create-collection("/db", "mongodbTest")
    let $token := mongodb:connect($support:mongoUrl)
    let $drop := mongodb:drop($token, $support:database, $support:mongoCollection)
    return xmldb:store($support:testCollection, $support:tokenStore, <token>{$token}</token>)
};

(: 
 : Disconnect from mongodb, cleanup token
 :)
declare function support:cleanup() {
    let $mongodbClientId := support:getToken()
    let $logout := mongodb:close($mongodbClientId)
    return
        xmldb:remove($support:testCollection)
};

(:  
 : Get token from store
 :)
declare function support:getToken() as xs:string {
    doc($support:testCollection || "/" || $support:tokenStore )//token/text()
};


