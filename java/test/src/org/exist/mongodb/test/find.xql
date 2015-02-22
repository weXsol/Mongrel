xquery version "3.0";

module namespace mongoMain="http://exist-db.org/mongodb/test/find";

import module namespace xqjson = "http://xqilla.sourceforge.net/lib/xqjson";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace mongodb = "http://exist-db.org/mongrel/mongodb" 
                at "java:org.exist.mongodb.xquery.MongodbModule";

import module namespace support = "http://exist-db.org/ext/mongodb/test/support"
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
                   "}"),
                   
                   mongodb:insert($mongodbClientId, $support:database, $support:mongoCollection, 
                   "{ " || 
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

(: collection#find() :)
declare 
    %test:assertEquals(20)
function mongoMain:find() {
    let $mongodbClientId := support:getToken()
    let $result := mongodb:find($mongodbClientId, $support:database, $support:mongoCollection)
    return
        count($result)
};


(: collection#find(query) :)
declare 
    %test:assertEquals(2)
function mongoMain:find_query() {
    let $mongodbClientId := support:getToken()
    let $result := mongodb:find($mongodbClientId, $support:database, $support:mongoCollection,
    "{ x : 5 }"
    )
    let $count := count($result)
    return $count
};

(: collection#find(query, keys) : only y-values are returned :)
declare 
    %test:assertEquals(2, 0, 2, 6, 4)
function mongoMain:find_query_keys() {

    let $mongodbClientId := support:getToken()
    let $result := mongodb:find($mongodbClientId, $support:database, $support:mongoCollection,
    "{ x : 6 }" , "{ y : 1 }")

    
    let $count := count($result)
    
    let $xValues := for $one in $result 
                    return xqjson:parse-json($one)//pair[@name eq 'x']/text()
                    
    let $yValues := for $one in $result 
                    return xqjson:parse-json($one)//pair[@name eq 'y']/text()
    return ($count, count($xValues), count($yValues), $yValues)
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

