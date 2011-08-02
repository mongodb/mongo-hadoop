
//use admin;



db.runCommand({addshard : "localhost:20001", allowLocal : true});
db.runCommand({addshard : "localhost:20002", allowLocal : true});
db.runCommand({addshard : "foo/localhost:20003,localhost:20004", allowLocal : true});


db.runCommand({"enablesharding" : "test"});

//The shard column gets an index added to it if it doesn't already have one
db.runCommand( { shardcollection : "test.lines", key : {num : 1} } );
//db.runCommand( { shardcollection : "test.lines", key : {_id : 1} } );



