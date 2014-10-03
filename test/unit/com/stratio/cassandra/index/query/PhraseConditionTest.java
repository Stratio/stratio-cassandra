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
package com.stratio.cassandra.index.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stratio.cassandra.index.schema.ColumnMapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

import com.stratio.cassandra.index.schema.ColumnMapperBoolean;
import com.stratio.cassandra.index.schema.Schema;

public class PhraseConditionTest
{

    @Test
    public void testPhraseQuery()
    {

        Map<String, ColumnMapper<?>> map = new HashMap<>();
        map.put("name", new ColumnMapperBoolean());
        Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

        List<String> values = new ArrayList<>();
        values.add("hola");
        values.add("adios");

        PhraseCondition phraseCondition = new PhraseCondition(0.5f, "name", values, 2);
        Query query = phraseCondition.query(mappers);
        Assert.assertNotNull(query);
        Assert.assertEquals(org.apache.lucene.search.PhraseQuery.class, query.getClass());
        org.apache.lucene.search.PhraseQuery luceneQuery = (org.apache.lucene.search.PhraseQuery) query;
        Assert.assertEquals(values.size(), luceneQuery.getTerms().length);
        Assert.assertEquals(2, luceneQuery.getSlop());
        Assert.assertEquals(0.5f, query.getBoost(), 0);
    }

}
