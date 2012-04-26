#!/usr/bin/env node

var node_mongo_hadoop = require('node_mongo_hadoop')

function reduceFunc(key, values, callback){
    var count = 0;
    values.forEach(function(v){
        count += v.count
    });
    callback( {'_id':key, 'count':count } );
}

node_mongo_hadoop.ReduceBSONStream(reduceFunc);
