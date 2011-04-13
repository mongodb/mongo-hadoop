
//use admin;



db.runCommand({addshard : "localhost:20011", allowLocal : true});
db.runCommand({addshard : "localhost:20012", allowLocal : true});
db.runCommand({addshard : "foo/localhost:20013,localhost:20014", allowLocal : true});


db.runCommand({"enablesharding" : "test"});

//The shard column gets an index added to it if it doesn't already have one
db.runCommand( { shardcollection : "test.weblog", key : {_id : 1} } );




