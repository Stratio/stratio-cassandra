package org.apache.cassandra.db.index.stratio.schema;

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
import org.apache.cassandra.db.index.stratio.AnalyzerFactory;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class for several culumns mappings between Cassandra and Lucene.
 * 
 * @author adelapena
 * 
 */
public class CellsMapper {

	/** The default Lucene's analyzer to be used if no other specified. */
	@JsonIgnore
	public static final Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_46);

	/** The Lucene's {@link corg.apache.lucene.analysis.Analyzer} class name. */
	@JsonProperty("default_analyzer")
	private final String defaultAnalyzerClassName;

	/** The Lucene's {@link corg.apache.lucene.analysis.Analyzer}. */
	@JsonIgnore
	private final Analyzer defaultAnalyzer;

	/** The per field Lucene's analyzer to be used. */
	@JsonIgnore
	private final PerFieldAnalyzerWrapper perFieldAnalyzer;

	/** The cell mappers. */
	@JsonProperty("fields")
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
	public CellsMapper(@JsonProperty("default_analyzer") String analyzerClassName,
	                   @JsonProperty("fields") Map<String, CellMapper<?>> cellMappers) {

		// Copy lower cased mappers
		this.cellMappers = cellMappers;

		// Setup default analyzer
		if (analyzerClassName == null) {
			this.defaultAnalyzer = DEFAULT_ANALYZER;
			this.defaultAnalyzerClassName = DEFAULT_ANALYZER.getClass().getName();
		} else {
			this.defaultAnalyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
			this.defaultAnalyzerClassName = analyzerClassName;
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
	@JsonIgnore
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
	@JsonIgnore
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

		int clusteringPosition = metadata.getCfDef().columns.size();
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
	@JsonIgnore
	public PerFieldAnalyzerWrapper analyzer() {
		return perFieldAnalyzer;
	}

	@JsonIgnore
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
		System.out.println("GETTING MAPPER FOR " + field);
		CellMapper<?> cellMapper = cellMappers.get(field.toLowerCase());
		if (cellMapper == null) {
			String[] components = field.split("\\.");
			System.out.println("SPLITTED " + ArrayUtils.toString(components));
			if (components.length < 2) {
				throw new IllegalArgumentException("Not found mapper for field " + field);
			}
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < components.length - 1; i++) {
				sb.append(components[i]);
				if (i < components.length - 2)
					sb.append(".");
			}
			return getMapper(sb.toString());
		} else {
			return cellMapper;
		}
	}

	/**
	 * Returns the {@link CellsMapper} contained in the specified JSON {@code String}.
	 * 
	 * @param json
	 *            A {@code String} containing the JSON representation of the {@link CellsMapper} to
	 *            be parsed.
	 * @return The {@link CellsMapper} contained in the specified JSON {@code String}.
	 */
	@JsonIgnore
	public static CellsMapper fromJson(String json) {
		try {
			return JsonSerializer.fromString(json, CellsMapper.class);
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
