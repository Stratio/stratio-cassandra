package org.apache.cassandra.db.index.stratio.schema;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
import org.apache.cassandra.db.index.stratio.query.BooleanQuery;
import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * Class for mapping between Cassandra's columns and Lucene's documents.
 * 
 * @author adelapena
 * 
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = CellMapperBoolean.class, name = "boolean"),
               @JsonSubTypes.Type(value = CellMapperDate.class, name = "date"),
               @JsonSubTypes.Type(value = CellMapperDouble.class, name = "double"),
               @JsonSubTypes.Type(value = CellMapperFloat.class, name = "float"),
               @JsonSubTypes.Type(value = CellMapperInteger.class, name = "integer"),
               @JsonSubTypes.Type(value = CellMapperLong.class, name = "long"),
               @JsonSubTypes.Type(value = CellMapperString.class, name = "string"),
               @JsonSubTypes.Type(value = CellMapperText.class, name = "text"),
               @JsonSubTypes.Type(value = CellMapperUUID.class, name = "uuid"), })
public abstract class CellMapper<BASE> {

	protected static final Analyzer EMPTY_ANALYZER = new KeywordAnalyzer();

	protected static final Store STORE = Store.NO;

	protected CellMapper() {

	}

	public static Cell build(String name, Object value) {
		return new Cell(name, value);
	}

	public static Cell cell(String name, ByteBuffer value, AbstractType<?> type) {
		return new Cell(name, type.compose(value));
	}

	public abstract Analyzer analyzer();

	/**
	 * Returns the Lucene's {@link org.apache.lucene.document.Field} resulting from the mapping of
	 * {@code value}, using {@code name} as field's name.
	 * 
	 * @param name
	 *            The name of the Lucene's field.
	 * @param value
	 *            The value of the Lucene's field.
	 * @return The Lucene's {@link org.apache.lucene.document.Field} resulting from the mapping of
	 *         {@code value}, using {@code name} as field's name.
	 */
	public abstract Field field(String name, Object value);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link AbstractQuery}.
	 * 
	 * @param abstractQuery
	 *            An abstract query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link AbstractQuery}.
	 */
	public Query query(AbstractQuery abstractQuery) {
		if (abstractQuery instanceof MatchQuery) {
			return query((MatchQuery) abstractQuery);
		} else if (abstractQuery instanceof PrefixQuery) {
			return query((PrefixQuery) abstractQuery);
		} else if (abstractQuery instanceof WildcardQuery) {
			return query((WildcardQuery) abstractQuery);
		} else if (abstractQuery instanceof PhraseQuery) {
			return query((PhraseQuery) abstractQuery);
		} else if (abstractQuery instanceof FuzzyQuery) {
			return query((FuzzyQuery) abstractQuery);
		} else if (abstractQuery instanceof RangeQuery) {
			return query((RangeQuery) abstractQuery);
		} else if (abstractQuery instanceof BooleanQuery) {
			return query((BooleanQuery) abstractQuery);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link MatchQuery}.
	 * 
	 * @param matchQuery
	 *            A match query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link MatchQuery}.
	 */
	protected abstract Query query(MatchQuery matchQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link PrefixQuery}.
	 * 
	 * @param matchQuery
	 *            A match query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link PrefixQuery}.
	 */
	protected abstract Query query(PrefixQuery prefixQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link WildcardQuery}.
	 * 
	 * @param wildcardQuery
	 *            A wildcard query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link WildcardQuery}.
	 */
	protected abstract Query query(WildcardQuery wildcardQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link PhraseQuery}.
	 * 
	 * @param phraseQuery
	 *            A phrase query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link PhraseQuery}.
	 */
	protected abstract Query query(PhraseQuery phraseQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link FuzzyQuery}.
	 * 
	 * @param fuzzyQuery
	 *            A fuzzy query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link FuzzyQuery}.
	 */
	protected abstract Query query(FuzzyQuery fuzzyQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link RangeQuery}.
	 * 
	 * @param rangeQuery
	 *            A range query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link RangeQuery}.
	 */
	protected abstract Query query(RangeQuery rangeQuery);

	/**
	 * Returns the cell value resulting from the mapping of the specified object.
	 * 
	 * @param value
	 *            The object to be mapped.
	 * @return The cell value resulting from the mapping of the specified object.
	 */
	public abstract BASE indexValue(Object value);

	public abstract BASE queryValue(Object value);
}
