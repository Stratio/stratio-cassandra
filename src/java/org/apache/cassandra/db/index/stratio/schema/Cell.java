package org.apache.cassandra.db.index.stratio.schema;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;

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

	private String nameSufix;

	private ByteBuffer value;

	private AbstractType<?> type;

	public Cell(String name, ByteBuffer value, AbstractType<?> type) {
		this.name = name;
		this.value = value;
		this.type = type;
	}

	public Cell(String name, String nameSufix, ByteBuffer value, AbstractType<?> type) {
		this.name = name;
		this.nameSufix = nameSufix;
		this.value = value;
		this.type = type;
	}

	/**
	 * Returns the name.
	 * 
	 * @return the name.
	 */
	public String getName() {
		return name;
	}

	public String getFieldName() {
		return nameSufix == null ? name : name + "." + nameSufix;
	}

	/**
	 * Returns the value.
	 * 
	 * @return the value.
	 */
	public ByteBuffer getRawValue() {
		return value;
	}

	public Object getValue() {
		return type.compose(value);
	}

	public AbstractType<?> getType() {
		return type;
	}

}
