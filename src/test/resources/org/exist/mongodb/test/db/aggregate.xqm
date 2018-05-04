xquery version "3.1";

module namespace aggregate = "http://exist-db.org/mongodb/test/aggregate";


import module namespace mongodb = "http://expath.org/ns/mongo";
import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "resource:org/exist/mongodb/test/db/support.xqm";
import module namespace test = "http://exist-db.org/xquery/xqsuite";
 
(: 
 :  Example test taken from http://docs.mongodb.org/ecosystem/tutorial/use-aggregation-framework-with-java-driver/ 
 :)              

(: Connect to mongodb, store token :)
declare %test:setUp function aggregate:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    return
        (
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            '{ "employee" : 1 , "department" : "Sales" , "amount" : 71 , "type" : "airfare"}'),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                            '{ "employee" : 2 , "department" : "Engineering" , "amount" : 15 , "type" : "airfare"}'),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                             '{ "employee" : 4 , "department" : "Human Resources" , "amount" : 5 , "type" : "airfare"}'),
            mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection,  
                             '{ "employee" : 42 , "department" : "Sales" , "amount" : 77 , "type" : "airfare"}')
        )
            
};


(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function aggregate:cleanup()
{   
    support:cleanup()
};


(: ----------------------------
 : Actual tests below this line  
 : ---------------------------- :)

(: 
 : collection#aggregate(query) 
 : 
 : { "_id" : "Human Resources" , "average" : 5.0} 
 : { "_id" : "Engineering" , "average" : 15.0} 
 : { "_id" : "Sales" , "average" : 74.0}
 :)
declare 
    %test:assertEquals("5.0", "15.0", "74.0") 
function aggregate:aggregate_simple() {
    let $mongodbClientId := support:getToken()
    
    let $result := mongodb:aggregate($mongodbClientId, $support:database, $support:mongoCollection,
                   (
                        '{ "$match" : { "type" : "airfare"}}', 
                        '{ "$project" : { "department" : 1 , "amount" : 1 , "_id" : 0}}',
                        '{ "$group" : { "_id" : "$department" , "average" : { "$avg" : "$amount"}}}',
                        '{ "$sort" : { "amount" : -1}}'   )
                    )

    for $one in $result
    return
        $formatted//pair[@name eq 'average']/text()
};

declare 
    %test:assertEquals(5, 15, 74) 
function aggregate:aggregate_simple_xq31() {
    let $mongodbClientId := support:getToken()
    
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    
    let $match := parse-json('{ "$match" : { "type" : "airfare"}}', $options)
    let $project := parse-json('{ "$project" : { "department" : 1 , "amount" : 1 , "_id" : 0}}', $options)
    let $group := parse-json('{ "$group" : { "_id" : "$department" , "average" : { "$avg" : "$amount"}}}', $options)
    let $sort := parse-json('{ "$sort" : { "amount" : -1}}', $options)
    
    let $result := mongodb:aggregate($mongodbClientId, $support:database, $support:mongoCollection,
                   ($match, $project, $group, $sort))

    return
        for $one in $result
        return parse-json($one, $options)?average

};

(: 
 : Run same tests directly with command function
 :)
declare 
    %test:assertEquals(5, 15, 74) 
function aggregate:aggregate_command_xq31() {
    let $mongodbClientId := support:getToken()
    
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    
    
    let $command := '{
  aggregate: "mongodbTest",
  pipeline: [
    {
        "$match": {
            "type": "airfare"
        }
    },
    {
        "$project": {
            "_id": 0,
            "amount": 1,
            "department": 1
        }
    },
    {
        "$group": {
            "_id": "$department",
            "average": {
                "$avg": "$amount"
            }
        }
    },
    {
        "$sort": {
            "amount": -1
        }
    }
]
}'


    let $cmdresult := mongodb:command($mongodbClientId, $support:database, $command)
    let $result := parse-json($cmdresult, $options)

    return
        map:get($result, "result")?*?average

};
