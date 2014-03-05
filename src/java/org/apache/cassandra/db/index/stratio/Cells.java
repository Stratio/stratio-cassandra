package org.apache.cassandra.db.index.stratio;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A sorted list of CQL3 logic {@link Cell}s.
 * 
 * @author adelapena
 * 
 */
public class Cells implements Iterable<Cell> {

	/** The wrapped columns */
	private List<Cell> cells;

	/**
	 * Constructs an empty {@link Cell} list.
	 */
	Cells() {
		this.cells = new LinkedList<>();
	}

	/**
	 * Adds the specified {@link Cell} to the existing ones.
	 * 
	 * @param cell
	 *            the {@link Cell} to be added.
	 */
	public void add(Cell cell) {
		cells.add(cell);
	}

	/**
	 * Adds the specified {@link Cell}s to the existing ones.
	 * 
	 * @param cells
	 *            the {@link Cell}s to be added.
	 */
	public void addAll(Collection<Cell> cells) {
		this.cells.addAll(cells);
	}

	/**
	 * Adds the specified {@link Cell}s to the existing ones.
	 * 
	 * @param cells
	 *            the {@link Cell}s to be added.
	 */
	public void addAll(Cells cells) {
		for (Cell cell : cells) {
			this.cells.add(cell);
		}
	}

	/**
	 * Returns an iterator over the {@link Cell}s in storage sequence.
	 * 
	 * @return an iterator over the {@link Cell}s storage sequence
	 */
	public Iterator<Cell> iterator() {
		return cells.iterator();
	}

	/**
	 * Returns the number of {@link Cell}s in this list. If this list contains more than
	 * <tt>Integer.MAX_VALUE</tt> elements, returns <tt>Integer.MAX_VALUE</tt>.
	 * 
	 * @return the number of {@link Cell}s in this list
	 */
	public int size() {
		return cells.size();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Cells [cells=");
		builder.append(cells);
		builder.append("]");
		return builder.toString();
	}

}
