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

import com.stratio.cassandra.index.util.ComparatorChain;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.dht.Token;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

import java.util.Comparator;

/**
 * Tuple relating a {@link Document} to a search {@link ScoreDoc}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ScoredDocument
{

    private final ScoreDoc scoreDoc;
    private final Document document;

    private Token token;
    private DecoratedKey partitionKey;
    private CellName clusteringKey;
    private ComparatorChain<ScoredDocument> comparator;

    /**
     * Returns a new {@link ScoredDocument} composed by the specified {@link Document} and {@link ScoreDoc}.
     *
     * @param document A {@link Document}.
     * @param scoreDoc A {@link ScoreDoc}.
     */
    public ScoredDocument(Document document, ScoreDoc scoreDoc)
    {
        this.scoreDoc = scoreDoc;
        this.document = document;
    }

    /**
     * Returns the numeric {@link Document}.
     *
     * @return The numeric {@link Document}.
     */
    public Document getDocument()
    {
        return document;
    }

    /**
     * Returns the numeric {@link ScoreDoc}.
     *
     * @return The numeric {@link ScoreDoc}.
     */
    public ScoreDoc getScoreDoc()
    {
        return scoreDoc;
    }

    /**
     * Returns the numeric score.
     *
     * @return The numeric score.
     */
    public Float getScore()
    {
        return scoreDoc.score;
    }

    public DecoratedKey getPartitionKey(final PartitionKeyMapper partitionKeyMapper)
    {
        if (partitionKey == null)
        {
            partitionKey = partitionKeyMapper.decoratedKey(document);
        }
        return partitionKey;
    }

    public CellName getClusteringKey(final ClusteringKeyMapper clusteringKeyMapper)
    {
        if (clusteringKey == null)
        {
            clusteringKey = clusteringKeyMapper.cellName(document);
        }
        return clusteringKey;
    }

    public Token<?> getToken(final PartitionKeyMapper partitionKeyMapper)
    {
        if (token == null)
        {
            token = getPartitionKey(partitionKeyMapper).getToken();
        }
        return token;
    }

    public Comparator<ScoredDocument> comparator(final PartitionKeyMapper partitionKeyMapper, final ClusteringKeyMapper clusteringKeyMapper)
    {
        if (comparator == null)
        {
            comparator = new ComparatorChain<>();
            comparator.addComparator(new Comparator<ScoredDocument>()
            {
                @Override
                public int compare(ScoredDocument sd1, ScoredDocument sd2)
                {
                    Token t1 = sd1.getToken(partitionKeyMapper);
                    Token t2 = sd2.getToken(partitionKeyMapper);
                    return t1.compareTo(t2);
                }
            });
            comparator.addComparator(new Comparator<ScoredDocument>()
            {
                @Override
                public int compare(ScoredDocument sd1, ScoredDocument sd2)
                {
                    CellName name1 = sd1.getClusteringKey(clusteringKeyMapper);
                    CellName name2 = sd2.getClusteringKey(clusteringKeyMapper);
                    return clusteringKeyMapper.comparator().compare(name1, name2);
                }
            });
        }
        return comparator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("ScoredDocument [scoreDoc=%s, document=%s]", scoreDoc, document);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (obj.getClass() != getClass())
        {
            return false;
        }
        ScoredDocument rhs = (ScoredDocument) obj;
        return new EqualsBuilder()
                .append(this.scoreDoc, rhs.scoreDoc)
                .append(this.document, rhs.document)
                .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                .append(scoreDoc)
                .append(document)
                .toHashCode();
    }
}
