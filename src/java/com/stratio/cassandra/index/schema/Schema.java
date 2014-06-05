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
package com.stratio.cassandra.index.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.stratio.cassandra.index.AnalyzerFactory;
import com.stratio.cassandra.index.util.ByteBufferUtils;
import com.stratio.cassandra.index.util.JsonSerializer;

/**
 * Class for several culumns mappings between Cassandra and Lucene.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class Schema {

	/** The default Lucene's analyzer to be used if no other specified. */
	public static final Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_46);

	/** The Lucene's {@link corg.apache.lucene.analysis.Analyzer}. */
	private final Analyzer defaultAnalyzer;

	/** The per field Lucene's analyzer to be used. */
	private final PerFieldAnalyzerWrapper perFieldAnalyzer;

	/** The cell mappers. */
	private Map<String, CellMapper<?>> cellMappers;

	/**
	 * Builds a new {@code ColumnsMapper} for the specified analyzer and cell mappers.
	 * 
	 * @param analyzerClassName
	 *            The name of the class of the analyzer to be used.
	 * @param cellMappers
	 *            The {@link Cell} mappers to be used.
	 */
	@JsonCreator
	public Schema(@JsonProperty("default_analyzer") String analyzerClassName,
	              @JsonProperty("fields") Map<String, CellMapper<?>> cellMappers) {

		// Copy lower cased mappers
		this.cellMappers = cellMappers;

		// Setup default analyzer
		if (analyzerClassName == null) {
			this.defaultAnalyzer = DEFAULT_ANALYZER;
		} else {
			this.defaultAnalyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
		}

		// Setup per field analyzer
		Map<String, Analyzer> analyzers = new HashMap<>();
		for (Entry<String, CellMapper<?>> entry : cellMappers.entrySet()) {
			String name = entry.getKey();
			CellMapper<?> mapper = entry.getValue();
			Analyzer fieldAnalyzer = mapper.analyzer();
			if (fieldAnalyzer != null) {
				analyzers.put(name, fieldAnalyzer);
			}
		}
		perFieldAnalyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzers);
	}

	/**
	 * Checks if this is consistent with the specified column family metadata.
	 * 
	 * @param metadata
	 *            A column family metadata.
	 * @throws ConfigurationException
	 *             If this is not consistent with the specified column family metadata.
	 */
	public void validate(CFMetaData metadata) throws ConfigurationException {
		for (Entry<String, CellMapper<?>> entry : cellMappers.entrySet()) {

			String name = entry.getKey();
			CellMapper<?> cellMapper = entry.getValue();
			ByteBuffer columnName = UTF8Type.instance.decompose(name);

			ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnName);
			if (columnDefinition == null) {
				throw new RuntimeException("No column definition for mapper " + name);
			}

			AbstractType<?> type = columnDefinition.getValidator();
			if (!cellMapper.supports(type)) {
				throw new RuntimeException("Not supported type for mapper " + name);
			}
		}
	}

	/**
	 * Returns all the {@link Cell}s representing the CQL3 columns contained in the specified column
	 * family.
	 * 
	 * @param metadata
	 *            The column family metadata
	 * @param partitionKey
	 *            A partition key.
	 * @param columnFamily
	 *            A column family.
	 * @param timestamp
	 *            The operation time stamp.
	 * @return The cells contained in the specified columns.
	 */
	public Cells cells(CFMetaData metadata, DecoratedKey partitionKey, ColumnFamily columnFamily) {
		Cells cells = new Cells();
		cells.addAll(partitionKeyCells(metadata, partitionKey));
		cells.addAll(clusteringKeyCells(metadata, columnFamily));
		cells.addAll(regularCells(metadata, columnFamily));
		return cells;
	}

	/**
	 * Returns the {@link Cell}s representing the CQL3 cells contained in the specified partition
	 * key.
	 * 
	 * @param metadata
	 *            The indexed column family meta data.
	 * @param partitionKey
	 *            The partition key.
	 * @return the {@link Cell}s representing the CQL3 cells contained in the specified partition
	 *         key.
	 */
	private Cells partitionKeyCells(CFMetaData metadata, DecoratedKey partitionKey) {
		Cells cells = new Cells();
		AbstractType<?> rawKeyType = metadata.getKeyValidator();
		List<ColumnDefinition> columnDefinitions = metadata.partitionKeyColumns();
		for (ColumnDefinition columnDefinition : columnDefinitions) {
			String name = UTF8Type.instance.compose(columnDefinition.name);
			ByteBuffer[] components = ByteBufferUtils.split(partitionKey.key, rawKeyType);
			int position = position(columnDefinition);
			ByteBuffer value = components[position];
			AbstractType<?> valueType = rawKeyType.getComponents().get(position);
			cells.add(CellMapper.cell(name, value, valueType));
		}
		return cells;
	}

	/**
	 * Returns the clustering key {@link Cell}s representing the CQL3 cells contained in the
	 * specified column family. The clustering key, if exists, is contained in each {@link Cell} of
	 * {@code columnFamily}.
	 * 
	 * @param metadata
	 *            The indexed column family meta data.
	 * @param columnFamily
	 *            The column family.
	 * @return The clustering key {@link Cell}s representing the CQL3 columns contained in the
	 *         specified column family.
	 */
	private Cells clusteringKeyCells(CFMetaData metadata, ColumnFamily columnFamily) {
		Cells cells = new Cells();
		ByteBuffer rawName = columnFamily.iterator().next().name();
		AbstractType<?> rawNameType = metadata.comparator;
		List<ColumnDefinition> columnDefinitions = metadata.clusteringKeyColumns();
		for (ColumnDefinition columnDefinition : columnDefinitions) {
			String name = UTF8Type.instance.compose(columnDefinition.name);
			ByteBuffer[] components = ByteBufferUtils.split(rawName, rawNameType);
			int position = position(columnDefinition);
			ByteBuffer value = components[position];
			AbstractType<?> valueType = rawNameType.getComponents().get(position);
			cells.add(CellMapper.cell(name, value, valueType));
		}
		return cells;
	}

	/**
	 * Returns the regular {@link Cell}s representing the CQL3 columns contained in the specified
	 * column family.
	 * 
	 * @param metadata
	 *            The indexed column family meta data.
	 * @param columnFamily
	 *            The column family.
	 * @return The regular {@link Cell}s representing the CQL3 columns contained in the specified
	 *         column family.
	 */
	@SuppressWarnings("rawtypes")
	private Cells regularCells(CFMetaData metadata, ColumnFamily cf) {

		Cells cells = new Cells();

		// Get row's cells iterator skipping clustering column
		Iterator<Column> columnIterator = cf.iterator();
		columnIterator.next();

		// Stuff for grouping collection cells (sets, lists and maps)
		String name = null;
		CollectionType collectionType = null;

		//int clusteringPosition = metadata.getCfDef().columns.size();
		int clusteringPosition = metadata.getCfDef().clusteringColumnsCount(); 
		CompositeType nameType = (CompositeType) metadata.comparator;

		while (columnIterator.hasNext()) {

			Column column = columnIterator.next();

			ByteBuffer columnName = column.name();
			ByteBuffer columnValue = column.value();

			ByteBuffer[] columnNameComponents = nameType.split(columnName);
			ByteBuffer columnSimpleName = columnNameComponents[clusteringPosition];

			ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnSimpleName);
			final AbstractType<?> valueType = columnDefinition.getValidator();
			int position = position(columnDefinition);

			name = UTF8Type.instance.compose(columnDefinition.name);

			if (valueType.isCollection()) {
				collectionType = (CollectionType<?>) valueType;
				switch (collectionType.kind) {
					case SET: {
						AbstractType<?> type = collectionType.nameComparator();
						ByteBuffer value = ByteBufferUtils.split(column.name(), nameType)[position + 1];
						cells.add(CellMapper.cell(name, value, type));
						break;
					}
					case LIST: {
						AbstractType<?> type = collectionType.valueComparator();
						ByteBuffer value = column.value();
						cells.add(CellMapper.cell(name, value, type));
						break;
					}
					case MAP: {
						AbstractType<?> type = collectionType.valueComparator();
						AbstractType<?> keyType = collectionType.nameComparator();
						ByteBuffer keyValue = ByteBufferUtils.split(column.name(), nameType)[position + 1];
						ByteBuffer value = column.value();
						String nameSufix = keyType.compose(keyValue).toString();
						cells.add(CellMapper.cell(name, nameSufix, value, type));
						break;
					}
				}
			} else {
				cells.add(CellMapper.cell(name, columnValue, valueType));
			}
		}

		return cells;
	}

	private static int position(ColumnDefinition cd) {
		return cd.componentIndex == null ? 0 : cd.componentIndex;
	}

	/**
	 * Returns the used {@link PerFieldAnalyzerWrapper}.
	 * 
	 * @return The used {@link PerFieldAnalyzerWrapper}.
	 */
	public PerFieldAnalyzerWrapper analyzer() {
		return perFieldAnalyzer;
	}

	public void addFields(Document document, CFMetaData metadata, DecoratedKey partitionKey, ColumnFamily columnFamily) {
		Cells cells = cells(metadata, partitionKey, columnFamily);
		for (Cell cell : cells) {
			String name = cell.getName();
			String fieldName = cell.getFieldName();
			Object value = cell.getValue();
			CellMapper<?> cellMapper = cellMappers.get(name);
			if (cellMapper != null) {
				Field field = cellMapper.field(fieldName, value);
				document.add(field);
			}
		}
	}

	public CellMapper<?> getMapper(String field) {
		CellMapper<?> cellMapper = cellMappers.get(field);
		if (cellMapper == null) {
			String[] components = field.split("\\.");
			if (components.length < 2) {
				throw new IllegalArgumentException("Not found mapper for field " + field);
			}
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < components.length - 1; i++) {
				sb.append(components[i]);
				if (i < components.length - 2) {
					sb.append(".");
				}
			}
			return getMapper(sb.toString());
		} else {
			return cellMapper;
		}
	}

	/**
	 * Returns the {@link Schema} contained in the specified JSON {@code String}.
	 * 
	 * @param json
	 *            A {@code String} containing the JSON representation of the {@link Schema} to be
	 *            parsed.
	 * @return The {@link Schema} contained in the specified JSON {@code String}.
	 */
	public static Schema fromJson(String json) {
		try {
			return JsonSerializer.fromString(json, Schema.class);
		} catch (IOException e) {
			throw new IllegalArgumentException("Schema unparseable: " + json, e);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Schema [defaultAnalyzer=");
		builder.append(defaultAnalyzer);
		builder.append(", perFieldAnalyzer=");
		builder.append(perFieldAnalyzer);
		builder.append(", cellMappers=");
		builder.append(cellMappers);
		builder.append("]");
		return builder.toString();
	}

}
