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
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchResultBuilderWide implements SearchResultBuilder
{
    private final PartitionKeyMapper partitionKeyMapper;
    private final ClusteringKeyMapper clusteringKeyMapper;

    public SearchResultBuilderWide(PartitionKeyMapper partitionKeyMapper, ClusteringKeyMapper clusteringKeyMapper)
    {
        this.partitionKeyMapper = partitionKeyMapper;
        this.clusteringKeyMapper = clusteringKeyMapper;
    }

    public SearchResult build(Document document, ScoreDoc scoreDoc) {
        DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
        CellName clusteringKey = clusteringKeyMapper.cellName(document);
        return new SearchResult(partitionKey, clusteringKey, scoreDoc);
    }
}
