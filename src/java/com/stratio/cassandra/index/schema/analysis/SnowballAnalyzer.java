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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SnowballAnalyzer extends Analyzer {

    private final String name;
    private final CharArraySet stopwords;

    @JsonCreator
    public SnowballAnalyzer(@JsonProperty("language") String name, @JsonProperty("stopwords") String stopwords) {
        this.name = name;
        if (stopwords == null) {
            this.stopwords = getDefaultStopWords(name);
        } else {
            String[] stopwordsArray = stopwords.split(",");
            List<String> stopwordsList = new ArrayList<>(stopwordsArray.length);
            for (String stop : stopwordsArray) {
                stopwordsList.add(stop.trim());
            }
            this.stopwords = new CharArraySet(stopwordsList, true);
        }
    }

    public org.apache.lucene.analysis.Analyzer analyzer() {
        return new org.apache.lucene.analysis.Analyzer() {
            protected TokenStreamComponents createComponents(String field,
                                                             Reader reader) {
                final Tokenizer source = new StandardTokenizer(reader);
                TokenStream result = new StandardFilter(source);
                result = new LowerCaseFilter( result);
                result = new StopFilter(result, stopwords);
                result = new SnowballFilter(result, name);
                return new TokenStreamComponents(source, result);
            }
        };
    }

    static CharArraySet getDefaultStopWords(String name) {
        switch (name) {
            case "English": return EnglishAnalyzer.getDefaultStopSet();
            case "French": return FrenchAnalyzer.getDefaultStopSet();
            case "Spanish": return SpanishAnalyzer.getDefaultStopSet();
            case "Portuguese": return PortugueseAnalyzer.getDefaultStopSet();
            case "Italian": return ItalianAnalyzer.getDefaultStopSet();
            case "Romanian": return RomanianAnalyzer.getDefaultStopSet();
            case "German": return GermanAnalyzer.getDefaultStopSet();
            case "Dutch": return DutchAnalyzer.getDefaultStopSet();
            case "Swedish": return SwedishAnalyzer.getDefaultStopSet();
            case "Norwegian": return NorwegianAnalyzer.getDefaultStopSet();
            case "Danish": return DanishAnalyzer.getDefaultStopSet();
            case "Russian": return RussianAnalyzer.getDefaultStopSet();
            case "Finnish": return FinnishAnalyzer.getDefaultStopSet();
            case "Irish": return IrishAnalyzer.getDefaultStopSet();
            case "Czech": return CzechAnalyzer.getDefaultStopSet();
            case "Hungarian": return HungarianAnalyzer.getDefaultStopSet();
            case "Turkish": return SpanishAnalyzer.getDefaultStopSet();
            case "Armenian": return SpanishAnalyzer.getDefaultStopSet();
            case "Basque": return BasqueAnalyzer.getDefaultStopSet();
            case "Catalan": return CatalanAnalyzer.getDefaultStopSet();
            default: return CharArraySet.EMPTY_SET;
        }
    }
}
