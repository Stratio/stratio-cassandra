Stratio Cassandra
=================

Stratio Cassandra is a fork of [Apache Cassandra](http://cassandra.apache.org/) where index functionality has been extended to provide near real time search such as ElasticSearch or Solr, including [full text search](http://en.wikipedia.org/wiki/Full_text_search) capabilities and free multivariable search. It is achieved through an [Apache Lucene](http://lucene.apache.org/) based implementation of Cassandra secondary indexes, where each node of the cluster indexes its own data.

Index [relevance queries](http://en.wikipedia.org/wiki/Relevance_(information_retrieval)) allows you to retrieve **the *n* more relevant results** satisfying a query. The coordinator node sends the query to each node in the cluster, each node returns its *n* best results and then the coordinator combines these partial results and gives you the *n* best of them, avoiding full scan.

Index filtered queries are a powerful help when analyzing the data stored in Cassandra with [MapReduce](http://es.wikipedia.org/wiki/MapReduce) frameworks as [Apache Hadoop](http://hadoop.apache.org/) or, even better, [Apache Spark](http://spark.apache.org/) through [Stratio Deep](https://github.com/Stratio/stratio-deep). Adding Lucene filters in the jobs input queries can **dramatically reduce** the amount of data in the tasks input. It's possible because all the key/token restrictions and additional clauses in the CQL3 query are supported by the Stratio's index implementation. 

Any cell in the tables can be indexed, included the primary key ones both as collections. Wide rows are also supported. You can scan token/key ranges, apply additional CQL3 clauses and page on the filtered results.

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
  * Support for searching with key/token clauses
  * Support for searching with clauses with `ALLOW FILTERING`
  * Apache Hadoop support compatibility
  * [Stratio Deep](https://github.com/Stratio/stratio-deep) support compatibility
  * Self contained distribution
  
Not yet supported:

  * Thrift API
  * Legacy compact storage option
  * Indexing `counter` columns
  * Columns with TTL

Requirements
============

  * Java >= 1.7 (OpenJDK and Sun have been tested)
  * Ant >= 1.8

Building and running
====================

Stratio Cassandra is distributed as a fork of Apache Cassandra, so its building, installation, operation and maintenance is generally identical. To build and run type:

```
ant
sudo mkdir -p /var/log/cassandra
sudo chown -R `whoami` /var/log/cassandra
sudo mkdir -p /var/lib/cassandra
sudo chown -R `whoami` /var/lib/cassandra
bin/cassandra -f
```

Now you can do some tests using the Cassandra Query Language:

```
bin/cqlsh
```

The Lucene's index files will be stored in the same directories where the Cassandra ones will be. The default data directory is `/var/lib/cassandra/data`, and each index is placed next to the SStables of its indexed column family. 

For more details about Cassandra please see its [documentation](http://cassandra.apache.org/).

Example
=======

Given a table as follows to store tweets:

```
CREATE TABLE tweets (
    id INTEGER PRIMARY KEY,
    user INTEGER REFERENCES user(id),
    body VARCHAR(140),
    timestamp TIMESTAMP,
    lucene TEXT,
);
```

You can create a custom Lucene index on it with the following statement:

```
CREATE CUSTOM INDEX tweets_index ON tweets (lucene) 
USING 'org.apache.cassandra.db.index.stratio.RowIndex'
WITH OPTIONS = {
    'refresh_seconds'    : '1',
    'num_cached_filters' : '128',
    'ram_buffer_mb'      : '64',
    'max_merge_mb'       : '5',
    'max_cached_mb'      : '30',
    'schema' : '{
        default_analyzer : "org.apache.lucene.analysis.standard.StandardAnalyzer",
        fields : {`
            id        : {type : "integer"},
            user      : {type : "string"},
            body      : {type : "text",  analyzer : "org.apache.lucene.analysis.en.EnglishAnalyzer"},
            timestamp : {type : "date", pattern  : "yyyy/MM/dd"}
        }
    }'
};
```

This index will index all the columns in the table with the specified types, and will be refreshed once per second.

Now you can request, for example, the 100 more relevant tweets which *body* field contains the phrase "big data gives organizations":

```
SELECT * FROM tweets WHERE lucene='{
	query:{type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' limit 100;
```

If you want to search into the tweets in a certain date range, then you must add to the search a filter as follows:

```
SELECT * FROM tweets WHERE lucene='{
    filter:{type:"range", field:"timestamp", lower:"2014/04/25", upper:"2014/04/1"},
    query:{type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' limit 100;
```

If you want to refine the search to get only the tweets written only by users whose name starts with "a":

```
SELECT * FROM tweets WHERE lucene='{
    filter:{type:"boolean", must:[
				{type:"range", field:"timestamp", lower:"2014/04/25", upper:"2014/04/1"},
				{type:"prefix", field:"user", value:"a"},
			],
    query:{type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' limit 100;
```

Finally, if you want to restrict the search to a certain token range:

```
SELECT * FROM tweets WHERE lucene='{
    filter:{type:"boolean", must:[
				{type:"range", field:"timestamp", lower:"2014/04/25", upper:"2014/04/1"},
				{type:"prefix", field:"user", value:"a"},
			],
    query:{type:"phrase", field:"body", values:["big","data","gives","organizations"]}
}' AND token(key) >= token(0) AND token(key) < token(10000000) limit 100;
```

This last is the basis of support for Hadoop, Spark and other MapReduce frameworks.











