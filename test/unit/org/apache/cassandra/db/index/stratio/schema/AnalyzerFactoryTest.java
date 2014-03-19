package org.apache.cassandra.db.index.stratio.schema;

import junit.framework.Assert;

import org.apache.cassandra.db.index.stratio.AnalyzerFactory;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Test;

public class AnalyzerFactoryTest {

	@Test
	public void testGetAnalyzer() {
		String analyzerClassName = "org.apache.lucene.analysis.es.SpanishAnalyzer";
		Analyzer analyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
		Assert.assertEquals(analyzer.getClass().getName(), analyzerClassName);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetAnalyzerInvalidInexistent() {
		String analyzerClassName = "org.apache.lucene.analysis.es.LepeAnalyzer";
		Analyzer analyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
		Assert.assertEquals(analyzer.getClass().getSimpleName(), analyzerClassName);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetAnalyzerInvalidNull() {
		String analyzerClassName = null;
		Analyzer analyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
		Assert.assertEquals(analyzer.getClass().getSimpleName(), analyzerClassName);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetAnalyzerInvalidEmpty() {
		String analyzerClassName = "";
		Analyzer analyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
		Assert.assertEquals(analyzer.getClass().getSimpleName(), analyzerClassName);
	}

	@Test
	public void testCloseAll() {
		AnalyzerFactory.closeAll();
		AnalyzerFactory.getAnalyzer("org.apache.lucene.analysis.es.SpanishAnalyzer");
		AnalyzerFactory.closeAll();
		AnalyzerFactory.closeAll();
	}
}
