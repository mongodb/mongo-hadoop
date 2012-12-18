import mongo_manager
x = mongo_manager.ReplicaSetManager(home="/tmp/rs0", with_arbiter=True, num_members=3)
x.start_set(fresh=True)
primary = x.get_primary()[0]
mongo_manager.mongo_import(primary, "mongo_hadoop", "yield_historical.in", "/Users/mike/projects/mongo-hadoop/examples/treasury_yield/src/main/resources/yield_historical_in.json")
