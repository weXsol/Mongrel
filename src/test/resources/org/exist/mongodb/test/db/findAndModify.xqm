xquery version "3.1";

module namespace findAndModify = "http://exist-db.org/mongodb/test/findAndModify";


import module namespace mongodb = "http://expath.org/ns/mongo";
import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
import module namespace test = "http://exist-db.org/xquery/xqsuite";
                

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
                            "{ x : 5 ,  y : 50 , z : 300 }"),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 6 ,  y : 60 , z : 3000 }"),
          mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            "{ x : 7 ,  y : 70 , z : 3000 }") 
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
                   
    let $y1 := $result1?y 
    let $y2 := $result2?y
    
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
                   
    let $y1 := $result1?x 
    let $y2 := $result2?x
    
    return ($y1, $y2)
        
};

declare 
    %test:assertEquals(70,60)
function findAndModify:findAndModify_sort_xq3() {
    let $mongodbClientId := support:getToken()
    
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    
    let $query_1 := parse-json("{ z : 3000 }", $options)
    let $update_1 := parse-json("{ x : 2 ,  y : 20 , z : 400 }", $options)
    let $sort_1 := parse-json("{ y : -1 }", $options)
    
    let $result_1 := mongodb:findAndModify($mongodbClientId, $support:database, $support:mongoCollection,
                   $query_1, $update_1, $sort_1)
                   
    
    

    let $result_2 := mongodb:findAndModify($mongodbClientId, $support:database, $support:mongoCollection,
                   $query_1, $update_1, $sort_1)
                   
  
   
    return
        ($result_1?y , $result_2?y)
        
};

