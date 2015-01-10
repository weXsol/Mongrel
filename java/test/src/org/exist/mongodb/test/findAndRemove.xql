xquery version "3.0";

module namespace findAndRemove="http://exist-db.org/mongodb/test/findAndRemove";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/xquery/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/ext/mongodb/test/support"
                at "./support.xqm";
                

(: Connect to mongodb, store token :)
declare %test:setUp function findAndRemove:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    let $drop := mongodb:drop($mongodbClientId, $support:database, $support:mongoCollection)
    return
        (
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 1 ,  y : 10 , z : 100 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 2 ,  y : 20 , z : 200 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 3 ,  y : 20 , z : 300 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 4 ,  y : 40 , z : 300 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 5 ,  y : 50 , z : 500 }")
        )
            
};

(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function findAndRemove:cleanup()
{   
    support:cleanup()
};

(: 
 : Actual tests below this line  
 :)



(: 
 : collection#findAndRemove(query) 
 : 
 : Execute update twice, 1st time removed value is returned, 2nd time is empty sequence
 :)
declare 
    %test:assertEquals(20, 0)
function findAndRemove:findAndRemove_simple() {
    let $mongodbClientId := support:getToken()
    
    let $result1 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ x : 2 }")
    let $result2 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ x : 2 }")
                   
    let $y1 := xqjson:parse-json($result1)//pair[@name eq 'y']/text() 
    
    return ($y1, count($result2))
        
};

(: 
 : collection#findAndRemove(query) 
 : 
 : Similar test, more complex query
 :)
declare 
    %test:assertEquals(3, 0)
function findAndRemove:findAndRemove_double() {
    let $mongodbClientId := support:getToken()
    
    let $result1 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ y : 20 , z : 300 }")
    let $result2 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ y : 20 , z : 300 }")
                   
    let $y1 := xqjson:parse-json($result1)//pair[@name eq 'x']/text() 

    
    return ($y1, count($result2))
        
};


