xquery version "3.1";

module namespace mapreduce = "http://exist-db.org/mongodb/test/mapreduce";


import module namespace mongodb = "http://expath.org/ns/mongo";
import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
import module namespace test = "http://exist-db.org/xquery/xqsuite";
 
(: 
 :  example taken from http://docs.mongodb.org/manual/core/map-reduce/
 :)              

(: Connect to mongodb, store token :)
declare %test:setUp function mapreduce:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    let $drop := mongodb:drop($mongodbClientId, $support:database, $support:mongoCollection)
    
    return(mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,
                          ('{ cust_id : "A123" , amount : 500 , status : "A"  }',
                          '{ cust_id : "A123" , amount : 250 , status : "A"  }',
                          '{ cust_id : "B212" , amount : 200 , status : "A"  }',
                          '{ cust_id : "A123" , amount : 300 , status : "D"  }')))
            
};


(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function mapreduce:cleanup()
{   
    support:cleanup()
};


(: ----------------------------
 : Actual tests below this line  
 : ---------------------------- :)

(: 
 : collection#map-reduce()  simple
 :)
declare 
    %test:assertEquals("A123", "750.0", "B212", "200.0") 
function mapreduce:mapreduce_simple() {
    let $mongodbClientId := support:getToken()
    
    let $result := mongodb:map-reduce($mongodbClientId, $support:database, $support:mongoCollection,
                   "function() { emit( this.cust_id, this.amount); }",
                   "function(key, values) { return Array.sum( values ) } ",
                   (),(),
                   '{ status : "A" }' 
                    )

        
        
        let $formatted := <result>{for $one in $result
    return parse-json($one)}</result>
    
    return data($formatted//pair)
        
};

(: 
 : collection#map-reduce() write result in collection
 :)
declare 
    %test:assertEquals("A123", "750.0", "B212", "200.0") 
function mapreduce:mapreduce_write_collection() {
    let $mongodbClientId := support:getToken()
    
    let $result := mongodb:map-reduce($mongodbClientId, $support:database, $support:mongoCollection,
                   "function() { emit( this.cust_id, this.amount); }",
                   "function(key, values) { return Array.sum( values ) } ",
                   ("newCollection"),("reduce"),
                   '{ status : "A" }' 
                    )

    let $find := mongodb:find($mongodbClientId, $support:database, "newCollection")   
        
    let $formatted := <result>{for $one in $find
    return 
        parse-json($one)}</result>
    
    let $drop := mongodb:drop($mongodbClientId, $support:database, "newCollection")
    
    return data($formatted//pair)
        
};

(: 
 : collection#map-reduce() test command failure exception
 :)
declare 
    %test:assertError("mongodb:MONG0005")
function mapreduce:mapreduce_command_failure() {
    let $mongodbClientId := support:getToken()
    
    return mongodb:map-reduce($mongodbClientId, $support:database, $support:mongoCollection,
                   "function() { bug }",
                   "function(key, values) { return Array.sum( values ) } ",
                   ("newCollection"),("reduce"),
                   '{ status : "A" }' 
                    )
};
