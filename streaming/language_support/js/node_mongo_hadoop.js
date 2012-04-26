var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var stream   = require('stream')
var BSON = require('mongodb').BSON;
var Buffers = require('buffers');
var _ = require('underscore');

var debug = function(x){
    process.stderr.write(JSON.stringify(x));
}

var BSONInputStream = exports.BSONInputStream = function(bytestream){
    EventEmitter.call(this);
    if(bytestream == null){
        this.bytestream = process.stdin;
    }else{
        this.bytestream = bytestream;
    }
    this.bufs = new Buffers()

    var self = this;

    this.bytestream.pause();
    this.processChunk = function(chunk){
        var parseDocuments = function(){
            while(true){ 
                // keep parsing + emitting docs 
                // break out of loop when we no longer have enough data
                if(self.bufs.length >= 4){ 
                    var sizeBuf = self.bufs.slice(0, 4);
                    var bsonObjectSize = sizeBuf.readInt32LE(0);
                    if(self.bufs.length>=bsonObjectSize){
                        var docBuf = self.bufs.splice(0, bsonObjectSize);
                        self.emit("document", BSON.deserialize(docBuf.toBuffer()));
                    }else{
                        break;
                    }
                }else{
                    break;
                }
            }
        }

        this.bufs.push(chunk)
        parseDocuments();
    }

    this.bytestream.on("data", function(chunk){
        self.processChunk(chunk);
    })

    this.bytestream.on("end", function(chunk){
        self.emit("end");
    })

}
inherits(BSONInputStream, EventEmitter);

BSONInputStream.prototype.start = function(){
    this.bytestream.resume();
}

var BSONOutput = exports.BSONOutput = function(outputstream){
    if(outputstream == null){
        this.outputstream = process.stdout;
    }else{
        this.outputstream = outputstream;
    }
}

BSONOutput.prototype.write = function(doc){
    var outputBuffer = BSON.serialize(doc);
    this.outputstream.write(outputBuffer);
}

var MapBSONStream = exports.MapBSONStream = function(mapFunc, inputStream){
    if(inputStream==null){
        inputStream = new BSONInputStream();
    }
    outputStream = new BSONOutput();
    inputStream.on("document", function(doc){
        mapFunc(doc, function(mapResult){
            outputStream.write(mapResult);
        });
    });
    inputStream.start();
}

var ReduceBSONStream = exports.ReduceBSONStream = function(reduceFunc, inputStream){
    outputStream = new BSONOutput();
    if(inputStream == null){
        inputStream = new BSONInputStream();
    }
    var currentGroup = []
    var currentKey;
    inputStream.on("document", function(doc){
        var docKey = doc._id;
        if(!_.isEqual(docKey, currentKey)){
            if(currentKey != undefined){
                reduceFunc(currentKey, currentGroup, function(reduceResult){
                    outputStream.write(reduceResult);
                });
            }
            currentKey = docKey;
            currentGroup = [doc]
        }else{
            currentGroup.push(doc)
        }
    });

    inputStream.on("end", function(){
        if(currentKey != undefined){
            reduceFunc(currentKey, currentGroup, function(reduceResult){
                outputStream.write(reduceResult);
            });
        }
    });
    inputStream.start();
}
