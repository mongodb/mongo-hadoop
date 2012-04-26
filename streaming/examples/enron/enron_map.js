#!/usr/bin/env node

var node_mongo_hadoop = require('node_mongo_hadoop')


var trimString = function(str){
  return String(str).replace(/^\s+|\s+$/g, '');
}

function mapFunc(doc, callback){
    if(doc.headers && doc.headers.From && doc.headers.To){
        var from_field = doc['headers']['From']
        var to_field = doc['headers']['To']
        var recips = []
        to_field.split(',').forEach(function(to){
          callback( {'_id': {'f':from_field, 't':trimString(to)}, 'count': 1} )
        });
    }
}

node_mongo_hadoop.MapBSONStream(mapFunc);
