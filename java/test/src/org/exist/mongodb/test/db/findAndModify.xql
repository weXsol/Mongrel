xquery version "3.0";

module namespace findAndModify="http://exist-db.org/mongodb/test/findAndModify";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/mongrel/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "./support.xqm";
                

(: Connect to mongodb, store token :)
declare %test:setUp function findAndModify:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    let $drop := mongodb:drop($mongodbClientId, $support:database, $support:mongoCollection)
    return
        (
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 1 ,  y : 10 , z : 100 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 2 ,  y : 10 , z : 200 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 3 ,  y : 30 , z : 300 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 4 ,  y : 40 , z : 300 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 5 ,  y : 50 , z : 300 }")
        )
            
};

(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function findAndModify:cleanup()
{   
    support:cleanup()
};

(: 
 : Actual tests below this line  
 :)



(: 
 : collection#findAndModify(query, update) 
 : 
 : Execute update twice, 1st time old value is returned, 2nd time the updated value
 :)
declare 
    %test:assertEquals(10, 20)
function findAndModify:findAndModify_simple() {
    let $mongodbClientId := support:getToken()
    
    let $result1 := mongodb:findAndModify($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ x : 2 }", "{ x : 2 ,  y : 20 , z : 200 }")
    let $result2 := mongodb:findAndModify($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ x : 2 }", "{ x : 2 ,  y : 30 , z : 200 }")
                   
    let $y1 := xqjson:parse-json($result1)//pair[@name eq 'y']/text() 
    let $y2 := xqjson:parse-json($result2)//pair[@name eq 'y']/text() 
    
    return ($y1, $y2)
        
};

(: 
 : collection#findAndModify(query, update, sort) 
 : 
Similar test, reverse sorting
 :)
declare 
    %test:assertEquals(5, 3)
function findAndModify:findAndModify_sort() {
    let $mongodbClientId := support:getToken()
    
    let $result1 := mongodb:findAndModify($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ z : 300 }", "{ x : 2 ,  y : 20 , z : 400 }", "{ y : -1 }")
    let $result2 := mongodb:findAndModify($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ z : 300 }", "{ x : 2 ,  y : 30 , z : 400 }", "{ y : 1 }")
                   
    let $y1 := xqjson:parse-json($result1)//pair[@name eq 'x']/text() 
    let $y2 := xqjson:parse-json($result2)//pair[@name eq 'x']/text() 
    
    return ($y1, $y2)
        
};


