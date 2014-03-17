package org.apache.cassandra.db.index.stratio.query;

import java.io.IOException;

import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * The abstract base class for queries.
 * 
 * Instantiable subclasses are:
 * <ul>
 * <li> {@link MatchQuery}
 * <li> {@link RangeQuery}
 * <li> {@link BooleanQuery}
 * <li> {@link LuceneQuery}
 * </ul>
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = BooleanQuery.class, name = "boolean"),
               @JsonSubTypes.Type(value = MatchQuery.class, name = "match"),
               @JsonSubTypes.Type(value = RangeQuery.class, name = "range"),
               @JsonSubTypes.Type(value = PhraseQuery.class, name = "phrase"),
               @JsonSubTypes.Type(value = FuzzyQuery.class, name = "fuzzy"),
               @JsonSubTypes.Type(value = WildcardQuery.class, name = "wildcard"), })
public abstract class AbstractQuery {

	public static final float DEFAULT_BOOST = 1.0f;

	@JsonProperty("boost")
	protected final float boost;

	/**
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}.
	 */
	@JsonCreator
	public AbstractQuery(@JsonProperty("boost") Float boost) {
		this.boost = boost == null ? DEFAULT_BOOST : boost;
	}

	/**
	 * Returns the boost for this clause. Documents matching this clause will (in addition to the
	 * normal weightings) have their score multiplied by {@code boost}. The boost is 1.0 by default.
	 * 
	 * @return The boost for this clause.
	 */
	public float getBoost() {
		return boost;
	}

	/**
	 * Returns the JSON representation of this.
	 * 
	 * @return the JSON representation of this.
	 */
	public String toJSON() throws IOException {
		return JsonSerializer.toString(this);
	}

	/**
	 * Returns the {@link AbstractQuery} represented by the specified JSON.
	 * 
	 * @param json
	 *            the JSON to be parsed.
	 * @return the {@link AbstractQuery} represented by the specified JSON.
	 */
	public static <T extends AbstractQuery> T fromJSON(String json, Class<T> clazz) throws IOException {
		return JsonSerializer.fromString(json, clazz);
	}

	/**
	 * Returns the {@link AbstractQuery} represented by the specified JSON.
	 * 
	 * @param json
	 *            the JSON to be parsed.
	 * @return the {@link AbstractQuery} represented by the specified JSON.
	 */
	public static AbstractQuery fromJSON(String json) throws IOException {
		return JsonSerializer.fromString(json, AbstractQuery.class);
	}

	public abstract void analyze(Analyzer analyzer);

	protected Object analyze(String field, Object value, Analyzer analyzer) {

		if (!(value instanceof String)) {
			return value;
		}

		TokenStream source = null;
		try {
			source = analyzer.tokenStream(field, (String) value);
			source.reset();

			TermToBytesRefAttribute termAtt = source.getAttribute(TermToBytesRefAttribute.class);
			BytesRef bytes = termAtt.getBytesRef();

			if (!source.incrementToken())
				return null;
			termAtt.fillBytesRef();
			if (source.incrementToken())
				throw new IllegalArgumentException("analyzer returned too many terms for multiTerm term: " + value);
			source.end();
			return BytesRef.deepCopyOf(bytes).utf8ToString();
		} catch (IOException e) {
			throw new RuntimeException("Error analyzing multiTerm term: " + value, e);
		} finally {
			IOUtils.closeWhileHandlingException(source);
		}
	}

	// protected final String createFieldQuery(Analyzer analyzer,
	// BooleanClause.Occur operator,
	// String field,
	// String queryText,
	// boolean quoted,
	// int phraseSlop) {
	// assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;
	// // Use the analyzer to get all the tokens, and then build a TermQuery,
	// // PhraseQuery, or nothing based on the term count
	// CachingTokenFilter buffer = null;
	// TermToBytesRefAttribute termAtt = null;
	// PositionIncrementAttribute posIncrAtt = null;
	// int positionCount = 0;
	// boolean severalTokensAtSamePosition = false;
	// boolean hasMoreTokens = false;
	//
	// TokenStream source = null;
	// try {
	// source = analyzer.tokenStream(field, queryText);
	// source.reset();
	// buffer = new CachingTokenFilter(source);
	// buffer.reset();
	//
	// if (buffer.hasAttribute(TermToBytesRefAttribute.class)) {
	// termAtt = buffer.getAttribute(TermToBytesRefAttribute.class);
	// }
	// if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
	// posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
	// }
	//
	// if (termAtt != null) {
	// try {
	// hasMoreTokens = buffer.incrementToken();
	// while (hasMoreTokens) {
	// int positionIncrement = (posIncrAtt != null) ? posIncrAtt.getPositionIncrement() : 1;
	// if (positionIncrement != 0) {
	// positionCount += positionIncrement;
	// } else {
	// severalTokensAtSamePosition = true;
	// }
	// hasMoreTokens = buffer.incrementToken();
	// }
	// } catch (IOException e) {
	// // ignore
	// }
	// }
	// } catch (IOException e) {
	// throw new RuntimeException("Error analyzing query text", e);
	// } finally {
	// IOUtils.closeWhileHandlingException(source);
	// }
	//
	// // rewind the buffer stream
	// buffer.reset();
	//
	// BytesRef bytes = termAtt == null ? null : termAtt.getBytesRef();
	//
	// return bytes.utf8ToString();
	// }

}
