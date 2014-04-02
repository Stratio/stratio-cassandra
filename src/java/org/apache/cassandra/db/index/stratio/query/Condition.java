package org.apache.cassandra.db.index.stratio.query;

import java.io.IOException;

import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
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
 * <li> {@link BooleanCondition}
 * <li> {@link FuzzyCondition}
 * <li> {@link MatchCondition}
 * <li> {@link PhraseCondition}
 * <li> {@link PrefixQueryTest}
 * <li> {@link RangeCondition}
 * <li> {@link WildcardCondition}
 * </ul>
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = BooleanCondition.class, name = "boolean"),
               @JsonSubTypes.Type(value = MatchCondition.class, name = "match"),
               @JsonSubTypes.Type(value = RangeCondition.class, name = "range"),
               @JsonSubTypes.Type(value = PhraseCondition.class, name = "phrase"),
               @JsonSubTypes.Type(value = PrefixCondition.class, name = "prefix"),
               @JsonSubTypes.Type(value = FuzzyCondition.class, name = "fuzzy"),
               @JsonSubTypes.Type(value = RegexpCondition.class, name = "regexp"),
               @JsonSubTypes.Type(value = WildcardCondition.class, name = "wildcard"), })
public abstract class Condition {

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
	public Condition(@JsonProperty("boost") Float boost) {
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
	 * Returns the Lucene's {@link Query} representation of this condition.
	 * 
	 * @param cellsMapper
	 *            The {@link CellsMapper} to be used.
	 * @return The Lucene's {@link Query} representation of this condition.
	 */
	public abstract Query query(CellsMapper cellsMapper);

	/**
	 * Returns the Lucene's {@link Filter} representation of this condition.
	 * 
	 * @param cellsMapper
	 *            The {@link CellsMapper} to be used.
	 * @return The Lucene's {@link Filter} representation of this condition.
	 */
	public Filter filter(CellsMapper cellsMapper) {
		return new QueryWrapperFilter(query(cellsMapper));
	}

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

			if (!source.incrementToken()) {
				return null;
			}
			termAtt.fillBytesRef();
			if (source.incrementToken()) {
				throw new IllegalArgumentException("analyzer returned too many terms for multiTerm term: " + value);
			}
			source.end();
			return BytesRef.deepCopyOf(bytes).utf8ToString();
		} catch (IOException e) {
			throw new RuntimeException("Error analyzing multiTerm term: " + value, e);
		} finally {
			IOUtils.closeWhileHandlingException(source);
		}
	}

}
