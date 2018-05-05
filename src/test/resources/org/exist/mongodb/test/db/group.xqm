xquery version "3.1";

module namespace groupa = "http://exist-db.org/mongodb/test/group";


import module namespace mongodb = "http://expath.org/ns/mongo";
import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
import module namespace test = "http://exist-db.org/xquery/xqsuite"
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";
import module namespace util = "http://exist-db.org/xquery/util";
 
(: 
 :  example taken from http://docs.mongodb.org/manual/core/map-reduce/
 :)              

(: Connect to mongodb, store token :)
declare %test:setUp function groupa:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    
    return(mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,
                          ('{ cust_id : "A123" , amount : 500 , status : "A" , foo : "bar" }',
                          '{ cust_id : "A123" , amount : 250 , status : "A" , foo : "bar" }',
                          '{ cust_id : "B212" , amount : 200 , status : "A" , foo : "bar" }',
                          '{ cust_id : "A123" , amount : 300 , status : "D" , foo : "bar" }')))
            
};


(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function groupa:cleanup()
{   
    support:cleanup()
};


(: ----------------------------
 : Actual tests below this line  
 : ---------------------------- :)

(: 
 : collection#group()  simple
 :)
declare 
%test:assertEquals(3, "750.0", 3)  
function groupa:group_simple() {
    let $mongodbClientId := support:getToken()
    
    let $result := mongodb:group($mongodbClientId, $support:database, $support:mongoCollection,
                "{ cust_id : 1 , amount : 1 , status : 1}",
                "{ status : 'A'}",
                "{ total : 0 }",
                "function ( curr, result ) { result.total += curr.amount;   } ")
                
                let $a := util:log-system-out($result)

   return ( trace(count($result), "A" ) , $result?amount)
        
};


(: 
 : collection#group()  simple
 : the test makes no sense but at least the new params are validated
 :)
declare 
function groupa:group_simple_xq31() {
    
    let $mongodbClientId := support:getToken()
    
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    
    let $key := parse-json("{ cust_id : 1 , amount : 1 , status : 1}", $options)
    let $cond := parse-json("{ status : 'A'}", $options)
    let $initial := parse-json("{ total : 0 }", $options)
    
    let $result := mongodb:group($mongodbClientId, $support:database, $support:mongoCollection,
                $key, $cond, $initial,
                "function ( curr, result ) {
                     result.total += curr.amount;
                  }" )

   return $result
        
};

