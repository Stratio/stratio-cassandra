package com.stratio.cassandra.index.schema.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class AnalysisTest {

    @Test
    public void testDefaultConstructor() {
        Analysis analysis = new Analysis();
        Analyzer analyzer = analysis.getAnalyzer("English");
        Assert.assertEquals(EnglishAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testRegularConstructor() {
        Map<String, AnalyzerBuilder> analyzers = new HashMap<>();
        analyzers.put("custom", new ClasspathAnalyzerBuilder("org.apache.lucene.analysis.es.SpanishAnalyzer"));
        Analysis analysis = new Analysis(analyzers);

        Analyzer englishAnalyzer = analysis.getAnalyzer("English");
        Assert.assertEquals(EnglishAnalyzer.class, englishAnalyzer.getClass());

        Analyzer customAnalyzer = analysis.getAnalyzer("custom");
        Assert.assertEquals(SpanishAnalyzer.class, customAnalyzer.getClass());
    }

    @Test
    public void testRegularConstructorWithNullAnalyzers() {
        Analysis analysis = new Analysis(null);
        Analyzer analyzer = analysis.getAnalyzer("English");
        Assert.assertEquals(EnglishAnalyzer.class, analyzer.getClass());
    }

    @Test
    public void testRegularConstructorWithEmptyAnalyzers() {
        Map<String, AnalyzerBuilder> analyzers = new HashMap<>();
        Analysis analysis = new Analysis(analyzers);
        Analyzer analyzer = analysis.getAnalyzer("English");
        Assert.assertEquals(EnglishAnalyzer.class, analyzer.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNotExistent() {
        Analysis analysis = new Analysis();
        analysis.getAnalyzer("custom");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerNull() {
        Analysis analysis = new Analysis();
        analysis.getAnalyzer(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAnalyzerEmpty() {
        Analysis analysis = new Analysis();
        analysis.getAnalyzer(" \t");
    }
}
