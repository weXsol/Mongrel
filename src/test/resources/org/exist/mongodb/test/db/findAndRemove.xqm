xquery version "3.1";

module namespace findAndRemove = "http://exist-db.org/mongodb/test/findAndRemove";


import module namespace mongodb = "http://expath.org/ns/mongo";
import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
import module namespace test = "http://exist-db.org/xquery/xqsuite"
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";
                

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
    %test:pending
    %test:assertEquals(20, 0)
function findAndRemove:findAndRemove_simple() {
    let $mongodbClientId := support:getToken()
    
    let $result1 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ x : 2 }")
    let $result2 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ x : 2 }")
                   
    let $y1 := $result1?y
    
    return ($y1, count($result2))
        
};

(: 
 : collection#findAndRemove(query) 
 : 
 : Similar test, more complex query
 :)
declare
    %test:pending
    %test:assertEquals(3, 0)
function findAndRemove:findAndRemove_double() {
    let $mongodbClientId := support:getToken()
    
    let $result1 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ y : 20 , z : 300 }")
    let $result2 := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   "{ y : 20 , z : 300 }")
                   
    let $y1 := $result1?x

    
    return ($y1, count($result2))
        
};

(: 
 : collection#findAndRemove(query) 
 : 
 : Xquery31
 :)
declare 
    %test:assertEquals(50)
function findAndRemove:findAndRemove_xq31() {
    let $mongodbClientId := support:getToken()
    
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $query := parse-json("{ x : 5 }", $options)
    
    let $result := mongodb:findAndRemove($mongodbClientId, $support:database, $support:mongoCollection,
                   $query)

                   
    return $result?y
        
};

