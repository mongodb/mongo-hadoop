# MongoDB Hadoop Adapter

The MongoDB Hadoop Adapter is a plugin for Hadoop that provides Hadoop the ability to
use MongoDB as an input source and/or an output source. 


CURRENT RELEASE: 1.0.0

This release primarly supports Hadoop 1.0 or Cloudera CDH3 Update 3
(Which ships 0.20.2).  If you wish to use Hadoop Streaming with
MongoDB, please see the notes on Streaming Hadoop versions below.

This product only supports MongoDB 2.2+; although it should
work with 2.0.x. We cannot provide support for legacy MongoDB
builds.

**Note**: If you have questions please email the
[mongodb-user Mailing List](http://groups.google.com/group/mongodb-user),
rather than directly contacting contributors or maintainers.

## Maintainers

* Mike O'Brien <mikeo@10gen.com>

## Contributors
* Brendan McAdams <brendan@10gen.com>
* Eliot Horowitz <erh@10gen.com>
* Ryan Nitz <ryan@10gen.com>
* Russell Jurney (@rjurney) (Lots of significant Pig improvements)
* Sarthak Dudhara <sarthak.83@gmail.com> (BSONWritable comparable interface)
* Priya Manda <priyakanth024@gmail.com> (Test Harness Code)
* Rushin Shah <rushin10@gmail.com> (Test Harness Code)
* Joseph Shraibman <jks@iname.com> (Sharded Input Splits)
* Sumin Xia <xiasumin1984@gmail.com> (Sharded Input Splits)

## Support

You will need the MongoDB Java Driver 2.7.3+.

Issue tracking: https://jira.mongodb.org/browse/HADOOP/

Discussion: http://groups.google.com/group/mongodb-user/

Documentation and Build Details: http://api.mongodb.org/hadoop/MongoDB%2BHadoop+Connector.html
