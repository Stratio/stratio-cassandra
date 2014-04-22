/*
* Copyright 2014, Stratio.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.cassandra.db.index.stratio.schema;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A sorted list of CQL3 logic {@link Cell}s.
 * 
 * @author Andres de la Pena <adelapen@stratio.com>
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

	public Cell getCell(String name) {
		for (Cell cell : cells) {
			if (cell.getName().equals(name)) {
				return cell;
			}
		}
		return null;
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
