xquery version "3.0";

module namespace groupa="http://exist-db.org/mongodb/test/group";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/mongrel/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "./support.xqm";
 
(: 
 :  example taken from http://docs.mongodb.org/manual/core/map-reduce/
 :)              

(: Connect to mongodb, store token :)
declare %test:setUp function groupa:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    
    return(mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,
                          ('{ cust_id : "A123" , amount : 500 , status : "A"  }',
                          '{ cust_id : "A123" , amount : 250 , status : "A"  }',
                          '{ cust_id : "B212" , amount : 200 , status : "A"  }',
                          '{ cust_id : "A123" , amount : 300 , status : "D"  }')))
            
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
    (: %test:assertEquals("A123", "750.0", "B212", "200.0")  :)
function groupa:group_simple() {
    let $mongodbClientId := support:getToken()
    
    let $result := mongodb:group($mongodbClientId, $support:database, $support:mongoCollection,
                "{ cust_id : 1 , amount : 1 , status : 1}",
                "{ status : 'A'}",
                "{ total : 0 }",
                "function ( curr, result ) {
                     result.total += curr.amount;
                  }"
                
                    )

   return $result
        
};


