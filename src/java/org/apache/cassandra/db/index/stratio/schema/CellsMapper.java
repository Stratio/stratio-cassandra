package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.stratio.AnalyzerFactory;
import org.apache.cassandra.db.index.stratio.RowQueryParser;
import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
import org.apache.cassandra.db.index.stratio.query.BooleanQuery;
import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
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

		// Copy fields
		this.cellMappers = cellMappers;

		// Setup analyzer
		if (analyzerClassName == null) {
			this.defaultAnalyzer = DEFAULT_ANALYZER;
			this.defaultAnalyzerClassName = DEFAULT_ANALYZER.getClass().getName();
		} else {
			this.defaultAnalyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
			this.defaultAnalyzerClassName = analyzerClassName;
		}

		// Setup analyzer
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
		String lastName = null;
		CollectionType collectionType = null;
		Set<Object> set = new HashSet<>();
		List<Object> list = new LinkedList<>();
		Map<Object, Object> map = new HashMap<>();

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

			lastName = name;
			name = UTF8Type.instance.compose(columnDefinition.name);

			if (lastName != null && !name.equals(lastName)) {

				// Empty previous collections
				if (!set.isEmpty()) {
					cells.add(CellMapper.build(lastName, set));
					set = new HashSet<>();
				} else if (!list.isEmpty()) {
					cells.add(CellMapper.build(lastName, list));
					list = new LinkedList<>();
				} else if (!map.isEmpty()) {
					cells.add(CellMapper.build(lastName, map));
					map = new HashMap<>();
				}
			}

			if (valueType.isCollection()) {
				collectionType = (CollectionType<?>) valueType;
				switch (collectionType.kind) {
					case SET: {
						AbstractType<?> setItemType = collectionType.nameComparator();
						ByteBuffer setItemValue = ByteBufferUtils.split(column.name(), nameType)[position + 1];
						set.add(setItemType.compose(setItemValue));
						break;
					}
					case LIST: {
						AbstractType<?> listItemType = collectionType.valueComparator();
						ByteBuffer listItemValue = column.value();
						list.add(listItemType.compose(listItemValue));
						break;
					}
					case MAP: {
						AbstractType<?> mapValueType = collectionType.valueComparator();
						AbstractType<?> mapKeyType = collectionType.nameComparator();
						ByteBuffer mapKeyValue = ByteBufferUtils.split(column.name(), nameType)[position + 1];
						ByteBuffer mapValueValue = column.value();
						map.put(mapKeyType.compose(mapKeyValue), mapValueType.compose(mapValueValue));
						break;
					}
				}
			} else {
				cells.add(CellMapper.cell(name, columnValue, valueType));
			}
		}

		// Empty remaining collections
		if (!set.isEmpty()) {
			cells.add(CellMapper.build(name, set));
			set = new HashSet<>();
		} else if (!list.isEmpty()) {
			cells.add(CellMapper.build(name, list));
			list = new LinkedList<>();
		} else if (!map.isEmpty()) {
			cells.add(CellMapper.build(name, map));
			map = new HashMap<>();
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
		Cells cells = new Cells();
		cells.addAll(partitionKeyCells(metadata, partitionKey));
		cells.addAll(clusteringKeyCells(metadata, columnFamily));
		cells.addAll(regularCells(metadata, columnFamily));
		for (Cell cell : cells) {
			String name = cell.getName();
			Object value = cell.getValue();
			CellMapper<?> cellMapper = cellMappers.get(name);
			if (cellMapper != null) {
				if (value instanceof Map) {
					Map<?, ?> map = (Map<?, ?>) value;
					for (Entry<?, ?> entry : map.entrySet()) {
						Object entryKey = entry.getKey();
						Object entryValue = entry.getValue();
						String entryName = name + '.' + entryKey.toString();
						Field field = cellMapper.field(entryName, entryValue);
						document.add(field);
					}
				} else if (value instanceof Set) {
					Set<?> set = (Set<?>) value;
					for (Object entry : set) {
						Field field = cellMapper.field(name, entry);
						document.add(field);
					}
				} else if (value instanceof List) {
					List<?> list = (List<?>) value;
					for (Object entry : list) {
						Field field = cellMapper.field(name, entry);
						document.add(field);
					}
				} else {
					Field field = cellMapper.field(name, value);
					document.add(field);
				}
			}
		}
	}

	public CellMapper<?> getMapper(String field) {
		CellMapper<?> mapper = cellMappers.get(field);
		if (mapper == null) {
			return new CellMapperText(defaultAnalyzerClassName);
		} else {
			return mapper;
		}
	}

	/**
	 * Returns the Lucene's {@link Query} parsed from the specified {@code String}.
	 * 
	 * @param querySentence
	 *            The {@code String} to be parsed.
	 * @return The Lucene's {@link Query} parsed from the specified {@code String}.
	 */
	@JsonIgnore
	public Query query(String querySentence) throws IOException, ParseException {
		try {
			AbstractQuery abstractQuery = AbstractQuery.fromJSON(querySentence);
			abstractQuery.analyze(perFieldAnalyzer);
			return query(abstractQuery);
		} catch (IOException e) {
			QueryParser queryParser = new RowQueryParser(Version.LUCENE_46, "lucene", perFieldAnalyzer, this);
			queryParser.setAllowLeadingWildcard(true);
			queryParser.setLowercaseExpandedTerms(false);
			return queryParser.parse(querySentence);
		}
	}

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

	public Query query(WildcardQuery query) {
		String field = query.getField();
		CellMapper<?> mapper = getMapper(field);
		return mapper.query(query);
	}

	public Query query(MatchQuery query) {
		String field = query.getField();
		CellMapper<?> mapper = getMapper(field);
		return mapper.query(query);
	}

	public Query query(PrefixQuery query) {
		String field = query.getField();
		CellMapper<?> mapper = getMapper(field);
		return mapper.query(query);
	}

	public Query query(PhraseQuery query) {
		String field = query.getField();
		CellMapper<?> mapper = getMapper(field);
		return mapper.query(query);
	}

	public Query query(FuzzyQuery query) {
		String field = query.getField();
		CellMapper<?> mapper = getMapper(field);
		return mapper.query(query);
	}

	public Query query(RangeQuery query) {
		String field = query.getField();
		CellMapper<?> mapper = getMapper(field);
		return mapper.query(query);
	}

	public Query query(BooleanQuery booleanQuery) {
		org.apache.lucene.search.BooleanQuery luceneBooleanQuery = new org.apache.lucene.search.BooleanQuery();
		luceneBooleanQuery.setBoost(booleanQuery.getBoost());
		for (AbstractQuery query : booleanQuery.getMust()) {
			Query luceneQuery = query(query);
			luceneBooleanQuery.add(luceneQuery, Occur.MUST);
		}
		for (AbstractQuery query : booleanQuery.getShould()) {
			Query luceneQuery = query(query);
			luceneBooleanQuery.add(luceneQuery, Occur.SHOULD);
		}
		for (AbstractQuery query : booleanQuery.getNot()) {
			Query luceneQuery = query(query);
			luceneBooleanQuery.add(luceneQuery, Occur.MUST_NOT);
		}
		return luceneBooleanQuery;
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
