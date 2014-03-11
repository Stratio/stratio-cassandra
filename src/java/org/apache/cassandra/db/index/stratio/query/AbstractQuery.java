package org.apache.cassandra.db.index.stratio.query;

import java.io.IOException;

import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
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
               @JsonSubTypes.Type(value = LuceneQuery.class, name = "lucene"),
               @JsonSubTypes.Type(value = MatchQuery.class, name = "match"),
               @JsonSubTypes.Type(value = RangeQuery.class, name = "range"), })
public class AbstractQuery {

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

}
