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

import com.stratio.cassandra.util.Log;
import org.apache.lucene.analysis.Analyzer;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public enum AnalyzerFactory {

    INSTANCE;

    private Map<String, Analyzer> cache = new HashMap<>();

    public synchronized Analyzer get(String name) {

        Analyzer analyzer = cache.get(name);
        if (analyzer != null) return analyzer;

        analyzer = PreBuiltAnalyzers.get(name);
        if (analyzer == null) {
            try {
                Class<?> analyzerClass = Class.forName(name);
                Constructor<?> constructor = analyzerClass.getConstructor();
                analyzer = (org.apache.lucene.analysis.Analyzer) constructor.newInstance();
            } catch (Exception e) {
                String message = "Not found analyzer: " + name;
                Log.error(e, message);
                throw new IllegalArgumentException(message, e);
            }
        }

        cache.put(name, analyzer);
        return analyzer;
    }
}
