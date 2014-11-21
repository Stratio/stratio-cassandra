/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.index;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.lucene.search.ScoreDoc;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchResult
{
    private final DecoratedKey partitionKey;
    private final CellName clusteringKey;
    private final ScoreDoc scoreDoc;

    public SearchResult(DecoratedKey partitionKey, CellName clusteringKey, ScoreDoc scoreDoc)
    {
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.scoreDoc = scoreDoc;
    }

    public DecoratedKey getPartitionKey()
    {
        return partitionKey;
    }

    public CellName getClusteringKey()
    {
        return clusteringKey;
    }

    public ScoreDoc getScoreDoc()
    {
        return scoreDoc;
    }

    public Float getScore()
    {
        return scoreDoc.score;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchResult that = (SearchResult) o;

        return scoreDoc.doc == that.scoreDoc.doc;
    }

    @Override
    public int hashCode()
    {
        return scoreDoc.doc;
    }
}
