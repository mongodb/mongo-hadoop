import mongo_manager, sys

try:
    shard1 = mongo_manager.ReplicaSetManager(home="/tmp/rs0", with_arbiter=True, num_members=3)
    shard1.start_set(fresh=True)

    shard2 = mongo_manager.ReplicaSetManager(home="/tmp/rs1", with_arbiter=True, num_members=3)
    shard2.start_set(fresh=True)

    # config server
    z = mongo_manager.StandaloneManager(home="/tmp/config_db")  
    zhost = z.start_server(fresh=True)

    s = mongo_manager.MongosManager(home="/tmp/mongos")
    s.start_mongos(zhost, [h.get_shard_string() for h in (shard1,shard2)], noauth=False, fresh=True, addShards=True)

    mongo_manager.mongo_import(s.port, "testdb", "testcoll", "/Users/mike/projects/mongo-hadoop/examples/treasury_yield/src/main/resources/yield_historical_in.json")

    s_client = s.connection()
    s_client['admin'].command("enablesharding", "testdb")
    s_client['admin'].command("shardCollection", "testdb.testcoll", key={"_id":1})
    sys.exit(0)
except:
    sys.exit(1)

