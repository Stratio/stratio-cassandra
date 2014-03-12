package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
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
	 */
	public abstract Field field(String name, Object value);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link MatchQuery}.
	 * 
	 * @param matchQuery
	 *            A match query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link MatchQuery}.
	 */
	public abstract Query query(MatchQuery matchQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link WildcardQuery}.
	 * 
	 * @param wildcardQuery
	 *            A wildcard query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link WildcardQuery}.
	 */
	public abstract Query query(WildcardQuery wildcardQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link PhraseQuery}.
	 * 
	 * @param phraseQuery
	 *            A phrase query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link PhraseQuery}.
	 */
	public abstract Query query(PhraseQuery phraseQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link FuzzyQuery}.
	 * 
	 * @param fuzzyQuery
	 *            A fuzzy query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link FuzzyQuery}.
	 */
	public abstract Query query(FuzzyQuery fuzzyQuery);

	/**
	 * Returns the Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 * {@link RangeQuery}.
	 * 
	 * @param rangeQuery
	 *            A range query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link RangeQuery}.
	 */
	public abstract Query query(RangeQuery rangeQuery);

	/**
	 * Returns the Lucene's range {@link org.apache.lucene.search.Query} resulting from the mapping
	 * of the specified arguments.
	 * 
	 * @param name
	 *            The name of the Lucene's field.
	 * @param lower
	 *            The lower accepted value.
	 * @param upper
	 *            The upper accepted value.
	 * @param includeLower
	 *            If the upper value is accepted.
	 * @param includeUpper
	 *            If the lower value is accepted.
	 * @return The Lucene's range {@link org.apache.lucene.search.Query} resulting from the mapping
	 *         of the specified arguments.
	 */
	public abstract Query query(String name, String lower, String upper, boolean includeLower, boolean includeUpper);

	/**
	 * Returns the Lucene's match {@link org.apache.lucene.search.Query} resulting from the mapping
	 * of the specified arguments.
	 * 
	 * @param name
	 *            The name of the Lucene's field.
	 * @param value
	 *            The value.
	 * @return The Lucene's match {@link org.apache.lucene.search.Query} resulting from the mapping
	 *         of the specified arguments.
	 */
	public abstract Query query(String name, String value);

	/**
	 * Returns the cell value resulting from the mapping of the specified object.
	 * 
	 * @param value
	 *            The object to be mapped.
	 * @param value
	 *            The value.
	 * @return The cell value resulting from the mapping of the specified object.
	 */
	protected abstract BASE value(Object value);
}
