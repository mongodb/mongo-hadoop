package com.mongodb.hadoop.hive;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.Test;

import java.util.List;

import static java.lang.String.format;

public class Hive2Test extends BaseHadoopTest {
    @Test
    public void client() throws TException, InterruptedException {

        //        HiveConf hiveConf = new HiveConf();
        //        hiveConf.addResource(getDfsCluster().getConfiguration(0));
        //        hiveConf.addResource(getYarnCluster().getConfig());
        //        hiveConf.setVar(HiveConf.ConfVars.HADOOPBIN, HADOOP_HOME + "/bin/hadoop");
        //
        //        TServerSocket tServerSocket = new TServerSocket(10000, 1000 * hiveConf.getIntVar(ConfVars.SERVER_READ_SOCKET_TIMEOUT));
        //        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(tServerSocket)
        //                                           .processorFactory(new ThriftHiveProcessorFactory(null, hiveConf))
        //                                           .transportFactory(new TTransportFactory())
        //                                           .protocolFactory(new TBinaryProtocol.Factory())
        //                                           .minWorkerThreads(1)
        //                                           .maxWorkerThreads(5);
        //
        //        TServer server = new TThreadPoolServer(sargs);
        //        server.serve();
        //
        //
        //        HiveServer hiveServer = new HiveServer();
        //                hiveServer.init(hiveConf);
        //                hiveServer.start();
        //
        //        System.out.println("sleeping");
        //        Thread.sleep(5000);
        TSocket transport = new TSocket("127.0.0.1", 10000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        HiveClient client = new HiveClient(protocol);
        transport.open();
        client.execute(format("DROP TABLE %s", HiveTest.HIVE_TEST_TABLE));

        client.execute(format("CREATE TABLE %s %s\n"
                              + "ROW FORMAT DELIMITED\n"
                              + "FIELDS TERMINATED BY '\\t'\n"
                              + "STORED AS %s",
                              HiveTest.HIVE_TEST_TABLE,
                              HiveTest.TEST_SCHEMA,
                              HiveTest.HIVE_TABLE_TYPE
                             ));

        client.execute(format("DESCRIBE %s", HiveTest.HIVE_TEST_TABLE));
        Schema schema = client.getSchema();
        for (FieldSchema fieldSchema : schema.getFieldSchemas()) {
            System.out.printf("%15s", fieldSchema.getName());
        }
        System.out.println();
        List<String> all = client.fetchAll();
        for (String s : all) {
            String[] split = s.split("\t");
            for (String s1 : split) {
                System.out.printf("%15s", s1.trim());
            }
            System.out.println();
        }
    }

    @Override
    protected MongoClientURI getInputUri() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
