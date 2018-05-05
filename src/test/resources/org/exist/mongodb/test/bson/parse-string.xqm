xquery version "3.1";

module namespace json = "http://exist-db.org/mongodb/test/json";


import module namespace bson = "http://exist-db.org/mongrel/bson";
import module namespace test = "http://exist-db.org/xquery/xqsuite"
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";


(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : Parser test using a string with simple value
 :)
declare 
    %test:assertEquals('{ "x" : 1}')
function json:simple_bson() {
    bson:parse-as-string("{ x : 1 }")
};

(: 
 : Parser test the native json parser - simple value
 :)
declare 
    %test:assertEquals('{ "x" : 1.0}')
function json:simple_native() {
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $json := parse-json("{ x : 1 }", $options)
    
    return bson:parse-as-string($json)
};


(: 
 : Parser test with array in string
 :)
declare 
    %test:assertEquals('{ "x" : 1 , "y" : 2 , "z" : 3}')
function json:array_bson() {
    bson:parse-as-string("{ x : 1 , y : 2 , z : 3 }")
};



(: 
 : Parser test the native json parser - array
 :)
declare 
    %test:assertEquals('{ "x" : 1.0 , "y" : 2.0 , "z" : 3.0}')
function json:array_native() {
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $json := parse-json("{ x : 1 , y : 2 , z : 3 }", $options)
    
    return bson:parse-as-string($json)
};

(: 
 : map
 :)
declare 
    %test:assertEquals('{ "x" : { "a" : "1" , "b" : "2"}}')
function json:map_bson() {
    bson:parse-as-string('{ x : { a : "1" , b : "2"}}')
};


(: 
 : map
 :)
declare 
    %test:assertEquals('{ "x" : { "a" : "1" , "b" : "2"}}')
function json:map_native() {
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $json := parse-json('{ x : { a : "1" , b : "2"}}', $options)
    return bson:parse-as-string($json)
};

(: 
 : Array
 :)
declare 
    %test:assertEquals('{ "x" : [ { "a" : "1" , "b" : "2"} , { "c" : "3" , "d" : "4"}]}')
function json:arrays_bson() {
    bson:parse-as-string('{ "x" : [ {"a" : "1" , "b" : "2"}, {"c" : "3", "d" : "4"} ] }')
};

(: 
 : Array
 :)
declare 
    %test:assertEquals('{ "x" : [ { "a" : "1" , "b" : "2"} , { "c" : "3" , "d" : "4"}]}')
function json:arrays_native() {
    let $options := map { "liberal": true(), "duplicates": "use-last" }
    let $json := parse-json('{ "x" : [ {"a" : "1" , "b" : "2"}, {"c" : "3", "d" : "4"} ] }', $options)
    return bson:parse-as-string($json)
};