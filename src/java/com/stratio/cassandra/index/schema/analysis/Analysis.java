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

import com.stratio.cassandra.index.schema.mapping.ColumnMapper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing a Lucene analysis configuration.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Analysis {

    private final Map<String, Analyzer> analyzers;

    /**
     * Builds a new analysis configuration.
     *
     * @param analyzers The {@link Analyzer}s to be used.
     */
    public Analysis(Map<String, AnalyzerBuilder> analyzers) {

        this.analyzers = new HashMap<>();
        if (analyzers != null) {
            for (Map.Entry<String, AnalyzerBuilder> entry : analyzers.entrySet()) {
                String name = entry.getKey();
                Analyzer analyzer = entry.getValue().analyzer();
                this.analyzers.put(name, analyzer);
            }
        }
    }

    /**
     * Builds a new default analysis configuration.
     */
    public Analysis() {
        this(null);
    }

    public Analyzer getDefaultAnalyzer(String name) {
        return name == null ? PreBuiltAnalyzers.DEFAULT.get() : getAnalyzer(name);
    }

    /**
     * Returns the {@link Analyzer} identified by the specified name. If there is no analyzer with the specified name,
     * then it will be interpreted as a class name and it will be instantiated by reflection.
     *
     * {@link IllegalArgumentException} is thrown if there is no {@link Analyzer} with such name.
     *
     * @param name The name of the {@link Analyzer} to be returned.
     * @return The {@link Analyzer} identified by the specified name.
     */
    public Analyzer getAnalyzer(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Not null nor empty analyzer name required");
        }
        Analyzer analyzer = analyzers.get(name);
        if (analyzer == null) {
            analyzer = PreBuiltAnalyzers.get(name);
            if (analyzer == null) {
                try {
                    analyzer = (new ClasspathAnalyzerBuilder(name)).analyzer();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Not found analyzer: " + name);
                }
            }
            analyzers.put(name, analyzer);
        }
        return analyzer;
    }

}
