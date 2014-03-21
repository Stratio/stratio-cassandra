package org.apache.cassandra.db.index.stratio.schema;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
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
               @JsonSubTypes.Type(value = CellMapperUUID.class, name = "uuid"),
               @JsonSubTypes.Type(value = CellMapperBigDecimal.class, name = "bigdec"),
               @JsonSubTypes.Type(value = CellMapperBigInteger.class, name = "bigint"), })
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
	 * @param query
	 *            An abstract query.
	 * @return The Lucene's {@link org.apache.lucene.search.Query} represented by the specified
	 *         {@link AbstractQuery}.
	 */
	public abstract Query toLucene(AbstractQuery query);

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
