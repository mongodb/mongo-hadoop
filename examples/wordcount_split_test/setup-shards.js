
//use admin;



db.runCommand({addshard : "localhost:20001", allowLocal : true});
db.runCommand({addshard : "localhost:20002", allowLocal : true});
db.runCommand({addshard : "foo/localhost:20003,localhost:20004", allowLocal : true});


db.runCommand({"enablesharding" : "test"});

db.runCommand( { shardcollection : "test.lines", key : {_id : 1} } );
db.runCommand( { shardcollection : "test.lines2", key : {_id : 1} } );


