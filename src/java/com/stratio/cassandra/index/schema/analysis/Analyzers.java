/*
 * Copyright 2015, Stratio.
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
package com.stratio.cassandra.index.schema.analysis;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Analyzers {

    private final Map<String, org.apache.lucene.analysis.Analyzer> analyzers = new HashMap<>();

    public Analyzers(Map<String, Analyzer> analyzers) {
        if (analyzers != null) {
            for (Map.Entry<String, Analyzer> entry : analyzers.entrySet()) {
                this.analyzers.put(entry.getKey(), entry.getValue().analyzer());
            }
        }
    }

    public org.apache.lucene.analysis.Analyzer getAnalyzer(String name) {
        org.apache.lucene.analysis.Analyzer analyzer = analyzers.get(name);
        return analyzer == null ? AnalyzerFactory.INSTANCE.get(name) : analyzer;
    }

}
