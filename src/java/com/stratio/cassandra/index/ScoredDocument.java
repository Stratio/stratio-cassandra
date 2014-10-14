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

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

/**
 * Tuple relating a {@link Document} to a search {@link ScoreDoc}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ScoredDocument
{

    private final ScoreDoc scoreDoc;
    private final Document document;

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

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("ScoredDocument [scoreDoc=%s, document=%s]", scoreDoc, document);
    }

}
