Stratio Cassandra
=================
Stratio Cassandra is an open source (under the [Apache License, v2.0](http://www.apache.org/licenses/LICENSE-2.0.html "The Apache License, v2.0")) fork of [Apache Cassandra](http://cassandra.apache.org/) where index functionality has been extended to provide near real time search such as ElasticSearch or Solr, including [full text search](http://en.wikipedia.org/wiki/Full_text_search) capabilities and free multivariable search. It is achieved through an [Apache Lucene](http://lucene.apache.org/) based implementation of Cassandra secondary indexes, where each node of the cluster indexes its own data.

It allows you to make cluster-wide [relevance queries](http://en.wikipedia.org/wiki/Relevance_(information_retrieval)) both as apply filters. Any cell in the tables can be indexed, included the primary key ones both as collections. Wide rows are also supported. You can scan token/key ranges, apply additional CQL3 clauses and page on the filtered results.

When doing relevance search to retrieve **the *n* more relevant results** satisfying a query, the coordinator node sends the query to each node in the cluster. Each node returns its *n* best results and then the coordinator combines these partial results and returns the *n* best of them. This is achieved through a modified version of the Cassandra's storage proxy. When the user's query is a filter that does not require relevance search then the usual Cassandra's query distribution system is used.

Stratio indexes are a powerful help when analyzing the data stored in Cassandra with [MapReduce](http://es.wikipedia.org/wiki/MapReduce) frameworks as [Apache Hadoop](http://hadoop.apache.org/) or, even better, [Apache Spark](http://spark.apache.org/). Adding Lucene filters in the jobs input queries can **dramatically reduce** the amount of data in the tasks input. It's possible because all the key/token restrictions and additional clauses in the CQL3 query are supported by the Stratio's index implementation. 

Other information including documentation is available on the [Stratio documentation website.](http://wordpress.dev.strat.io/cassandra/extended-search-in-cassandra/)

Features
========
Stratio Cassandra and its integration with Lucene search technology provides:

  * Big data full text search
  * Relevance scoring and sorting
  * Complex boolean queries (and, or, not)
  * Near real time search
  * CQL3 support
  * Wide rows support
  * Partition and cluster composite keys support
  * Support for indexing columns part of primary key
  * Apache Hadoop support compatibility
  * Stratio Deep support compatibility
  * Self contained distribution
  
Not yet supported:

  * Thrift API
  * Legacy compact storage option
  * Type `counter`
  * Columns with TTL

Requirements
============
  * Java >= 1.7 (OpenJDK and Sun have been tested)
  * Ant >= 1.8

Building and running
====================
Stratio Cassandra is distributed as a fork of Apache Cassandra, so its building, installation, operation and maintenance is generally identical. To build and run type:

  * ant
  * sudo mkdir -p /var/log/cassandra
  * sudo chown -R `whoami` /var/log/cassandra
  * sudo mkdir -p /var/lib/cassandra
  * sudo chown -R `whoami` /var/lib/cassandra
  * bin/cassandra -f

Now you can do some tests using the Cassandra Query Language:

  * bin/cqlsh

The Lucene's index files will be stored in the same directories where the Cassandra ones will be. The default data directory is `/var/lib/cassandra/data`, and each index is placed next to the SStables of its indexed column family. 

For more details about Cassandra please see the its [documentation](http://cassandra.apache.org/).

Getting started
===============











