xquery version "3.0";

module namespace mongoMain="http://exist-db.org/mongodb/test/findOne";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/mongrel/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/mongrel/test/support"
                at "./support.xqm";
                

(: Connect to mongodb, store token :)
declare %test:setUp function mongoMain:setup()
{
    let $setup := support:setup()
    let $mongodbClientId := support:getToken()
    let $drop := mongodb:drop($mongodbClientId, $support:database, $support:mongoCollection)
    let $result := for $i in (1 to 10) 
                   return  
                       ( mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   "{ " || 
                    "x : " || $i || ", " ||
                    "y : " || $i || ", " ||
                    "z : " || ($i * $i) || 
                   "},{ " || 
                    "x : " || $i || ", " ||
                    "y : " || (10 - $i) || ", " ||
                    "z : " || ($i * $i) || 
                   "}")
                       )
                   
    return""
};

(: Disconnect from mongodb, cleanup token :)
declare %test:tearDown function mongoMain:cleanup()
{
    support:cleanup()
};

(: 
 : Actual tests below this line  
 :)

(: collection#findOne() :)
declare 
    %test:assertEquals(1,1)
function mongoMain:findOne() {
    let $mongodbClientId := support:getToken()
    let $result := mongodb:findOne($mongodbClientId, $support:database, $support:mongoCollection)
    return
        ( count($result), xqjson:parse-json($result)//pair[@name eq 'y']/text() )
};


(: collection#findOne(query) :)
declare 
    %test:assertEquals(1, 8)
function mongoMain:findOne_query() {
    let $mongodbClientId := support:getToken()
    let $result := mongodb:findOne($mongodbClientId, $support:database, $support:mongoCollection, "{ z : 64 }" )
    return
        ( count($result), xqjson:parse-json($result)//pair[@name eq 'x']/text() )
};

(: collection#findOne(query, keys) : only y-values are returned :)
declare 
    %test:assertEquals(1, 0, 1, 6)
function mongoMain:findOne_query_fields() {

    let $mongodbClientId := support:getToken()
    let $result := mongodb:findOne($mongodbClientId, $support:database, $support:mongoCollection,
    "{ x : 6 }" , "{ y : 1 }")

    
    let $count := count($result)
    
    (: no x values :)
    let $xValues := for $one in $result 
                    return xqjson:parse-json($one)//pair[@name eq 'x']/text()
    
    (: just one y value :)                
    let $yValues := for $one in $result 
                    return xqjson:parse-json($one)//pair[@name eq 'y']/text()
                    
    return ($count, count($xValues), count($yValues), $yValues)
};

(: collection#findOne(query, keys, orderby) : only y-values are returned :)
declare 
    %test:assertEquals(1, 1, 1, 7)
function mongoMain:findOne_query_fields_orderby() {

    let $mongodbClientId := support:getToken()
    let $result := mongodb:findOne($mongodbClientId, $support:database, $support:mongoCollection,
    "{ z : 49 }" , "{ x : 1 , y : 1 }",  "{ x : 1 }")

    
    let $count := count($result)
    
    let $xValues := for $one in $result 
                    return xqjson:parse-json($one)//pair[@name eq 'x']/text()
                    
    let $yValues := for $one in $result 
                    return xqjson:parse-json($one)//pair[@name eq 'y']/text()
                    
    let $zValues := for $one in $result 
                    return xqjson:parse-json($one)//pair[@name eq 'z']/text()
                    
    return ($count, count($xValues), count($yValues), $xValues)
};



(: 
{
    "_id": {
        "$oid": "54a909544e08cd54e4f11167"
    },
    "x": 6,
    "y": 4,
    "z": 36
}

<json type="object">
    <pair name="_id" type="object">
        <pair name="$oid" type="string">54a909b94e08cd54e4f11171</pair>
    </pair>
    <pair name="x" type="number">6</pair>
    <pair name="y" type="number">4</pair>
    <pair name="z" type="number">36</pair>
</json>
:)


