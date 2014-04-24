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
