package com.stratio.cassandra.index;

import org.apache.lucene.search.similarities.DefaultSimilarity;

public class NoIDFSimilarity extends DefaultSimilarity
{

    @Override
    public float idf(long docFreq, long numDocs)
    {
        return 1.0f;
    }
}
