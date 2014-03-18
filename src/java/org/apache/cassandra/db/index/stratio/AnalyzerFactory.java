package org.apache.cassandra.db.index.stratio;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

/**
 * Class for building Lucene's {@link org.apache.lucene.analysis.Analyzer}s. It uses an internal
 * cache which associates analyzer class names with analyzers.
 * 
 * @author adelapena
 * 
 */
public final class AnalyzerFactory {

	/** Analyzers cache, actually mocked without any eviction. */
	private static Map<String, Analyzer> analyzers = new LinkedHashMap<String, Analyzer>();

	/**
	 * Returns the {@link org.apache.lucene.analysis.Analyzer} identified by the specified class
	 * name. The specified class must be in classpath. Benefits from cache.
	 * 
	 * @param analyzerClassName
	 *            The analyzer class name.
	 * @return
	 */
	public static Analyzer getAnalyzer(String analyzerClassName) {
		Analyzer analyzer = analyzers.get(analyzerClassName);
		if (analyzer != null) {
			return analyzer;
		}
		try {
			Class<?> analyzerClass = Class.forName(analyzerClassName);
			Constructor<?> constructor = analyzerClass.getConstructor(Version.class);
			analyzer = (Analyzer) constructor.newInstance(Version.LUCENE_46);
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
