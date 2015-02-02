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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class for building Lucene {@link Analyzer}s. It uses an internal cache which associates analyzer class names with
 * analyzers.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public final class AnalyzerFactory {

    /** Analyzers cache, actually mocked without any eviction. */
    private static Map<String, Analyzer> analyzers = new LinkedHashMap<>();

    /**
     * Returns the {@link Analyzer} identified by the specified class name. The specified class must be in classpath.
     * Benefits from cache.
     *
     * @param analyzerClassName The analyzer class name.
     * @return The {@link Analyzer} identified by the specified class name.
     */
    public static Analyzer getAnalyzer(String analyzerClassName) {
        Analyzer analyzer = analyzers.get(analyzerClassName);
        if (analyzer != null) {
            return analyzer;
        }
        try {
            Class<?> analyzerClass = Class.forName(analyzerClassName);
            Constructor<?> constructor = analyzerClass.getConstructor(Version.class);
            analyzer = (Analyzer) constructor.newInstance(Version.LUCENE_48);
            analyzers.put(analyzerClassName, analyzer);
            return analyzer;
        } catch (Exception e) {
            throw new IllegalArgumentException("Analyzer not found: " + analyzerClassName, e);
        }
    }

    /**
     * Closes all the cached analyzers.
     */
    public static void closeAll() {
        for (Analyzer analyzer : analyzers.values()) {
            analyzer.close();
        }
        analyzers.clear();
    }

}
