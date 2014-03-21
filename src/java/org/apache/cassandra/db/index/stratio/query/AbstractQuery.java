package org.apache.cassandra.db.index.stratio.query;

import java.io.IOException;

import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * The abstract base class for queries.
 * 
 * Known subclasses are:
 * <ul>
 * <li> {@link BooleanQuery}
 * <li> {@link FuzzyQuery}
 * <li> {@link MatchQuery}
 * <li> {@link PhraseQuery}
 * <li> {@link PrefixQuery}
 * <li> {@link RangeQuery}
 * <li> {@link WildcardQuery}
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
               @JsonSubTypes.Type(value = PrefixQuery.class, name = "prefix"),
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

	// /**
	// * Returns the {@link AbstractQuery} represented by the specified JSON.
	// *
	// * @param json
	// * the JSON to be parsed.
	// * @return the {@link AbstractQuery} represented by the specified JSON.
	// */
	// public static <T extends AbstractQuery> T fromJSON(String json, Class<T> clazz) throws
	// IOException {
	// return JsonSerializer.fromString(json, clazz);
	// }
	//
	// /**
	// * Returns the {@link AbstractQuery} represented by the specified JSON.
	// *
	// * @param json
	// * the JSON to be parsed.
	// * @return the {@link AbstractQuery} represented by the specified JSON.
	// */
	// public static AbstractQuery fromJSON(String json) throws IOException {
	// return JsonSerializer.fromString(json, AbstractQuery.class);
	// }

	/**
	 * Returns the Lucene's {@link Query} representation of this query.
	 * 
	 * @param cellsMapper
	 *            The {@link CellsMapper} to be used.
	 * @return The Lucene's {@link Query} representation of this query.
	 */
	public abstract Query toLucene(CellsMapper cellsMapper);

	/**
	 * Applies the specified {@link Analyzer} to the required arguments.
	 * 
	 * @param analyzer
	 *            An {@link Analyzer}.
	 */
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

}
