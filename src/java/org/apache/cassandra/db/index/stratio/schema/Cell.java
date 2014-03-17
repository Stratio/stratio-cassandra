package org.apache.cassandra.db.index.stratio.schema;

/**
 * A cell of a CQL3 logic {@link Cell}, which in most cases is different from a storage engine
 * column.
 * 
 * @author adelapena
 * 
 */
public class Cell {

	/** The column's name */
	private String name;
	/** The column's value */
	private final Object value;

	/**
	 * Returns a new {@link Cell}.
	 * 
	 * @param name
	 *            the name.
	 * @param value
	 *            the value.
	 */
	public Cell(String name, Object value) {
		this.name = name;
		this.value = value;
	}

	/**
	 * Returns the name.
	 * 
	 * @return the name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the value.
	 * 
	 * @return the value.
	 */
	public Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" [name=");
		builder.append(name);
		builder.append(", value=");
		builder.append(value);
		builder.append("]");
		return builder.toString();
	}

}
