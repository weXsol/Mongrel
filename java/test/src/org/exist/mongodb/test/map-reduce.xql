xquery version "3.0";

module namespace mapreduce="http://exist-db.org/mongodb/test/mapreduce";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/xquery/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/ext/mongodb/test/support"
                at "./support.xqm";
 
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
    return xqjson:parse-json($one)}</result>
    
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
    return xqjson:parse-json($one)}</result>
    
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
