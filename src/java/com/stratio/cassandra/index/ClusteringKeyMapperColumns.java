package com.stratio.cassandra.index;

import com.stratio.cassandra.index.query.builder.MatchConditionBuilder;
import com.stratio.cassandra.index.query.builder.RangeConditionBuilder;
import com.stratio.cassandra.index.schema.ColumnMapper;
import com.stratio.cassandra.index.schema.ColumnMapperSingle;
import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * {@link ClusteringKeyMapper} that uses (if possible) {@link Schema} column mappers.
 * <p/>
 * This implementation has a good performance but is not applicable to any schema.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyMapperColumns extends ClusteringKeyMapper {
    private final Schema schema;
    private final String[] names;
    private final ColumnMapperSingle[] columnMappers;
    private final AbstractType[] types;
    private final int numClusteringColumns;

    private ClusteringKeyMapperColumns(Schema schema,
                                       CFMetaData metadata,
                                       String[] names,
                                       ColumnMapperSingle[] columnMappers,
                                       AbstractType[] types) {
        super(metadata);
        this.schema = schema;
        this.names = names;
        this.columnMappers = columnMappers;
        this.types = types;
        this.numClusteringColumns = metadata.clusteringColumns().size();
    }

    /**
     * Returns a new {@link ClusteringKeyMapperColumns} for the specified {@link CFMetaData} and {@link Schema}, or
     * {@code null} if this implementation is not able to manage the specified parameters.
     *
     * @param metadata A {@link CFMetaData}.
     * @param schema   A {@link Schema}.
     * @return A new {@link ClusteringKeyMapperColumns} for the specified {@link CFMetaData} and {@link Schema}, or
     * {@code null} if this implementation is not able to manage the specified parameters.
     */
    @SuppressWarnings("unchecked")
    public static ClusteringKeyMapperColumns instance(CFMetaData metadata, Schema schema) {
        List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
        int numClusteringColumns = clusteringColumns.size();
        if (numClusteringColumns == 0) {
            return null;
        }
        String[] names = new String[numClusteringColumns];
        ColumnMapperSingle[] columnMappers = new ColumnMapperSingle[numClusteringColumns];
        AbstractType[] types = new AbstractType[numClusteringColumns];
        for (int i = 0; i < numClusteringColumns; i++) {
            ColumnDefinition columnDefinition = clusteringColumns.get(i);
            String name = columnDefinition.name.toString();
            ColumnMapperSingle columnMapper = schema.getMapperSingle(name);
            if (columnMapper != null) {
                AbstractType type = columnDefinition.type;
                if (columnMapper.supportsClustering(type)) {
                    names[i] = name;
                    types[i] = type;
                    columnMappers[i] = columnMapper;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
        return new ClusteringKeyMapperColumns(schema, metadata, names, columnMappers, types);
    }

    @Override
    public SortField[] sortFields() {
        SortField[] sortFields = new SortField[numClusteringColumns];
        for (int i = 0; i < numClusteringColumns; i++) {
            String name = names[i];
            ColumnMapper columnMapper = columnMappers[i];
            sortFields[i] = columnMapper.sortField(name, false);
        }
        return sortFields;
    }

    private Object queryValue(Composite composite, int i) {
        ByteBuffer component = composite.get(i);
        String name = names[i];
        ColumnMapperSingle columnMapper = columnMappers[i];
        AbstractType<?> type = types[i];
        Object value = type.compose(component);
        return columnMapper.queryValue(name, value);
    }

    private boolean includeStart(Composite composite) {
        ByteBuffer[] components = ByteBufferUtils.split(composite.toByteBuffer(), compositeType);
        if (components.length > numClusteringColumns) {
            return false;
        } else {
            return composite.eoc() == Composite.EOC.NONE;
        }
    }

    private boolean includeStop(Composite composite) {
        ByteBuffer[] components = ByteBufferUtils.split(composite.toByteBuffer(), compositeType);
        if (components.length > numClusteringColumns) {
            return true;
        } else {
            return composite.eoc() == Composite.EOC.END;
        }
    }

    @Override
    public Query query(Composite start, Composite stop) {
        BooleanQuery booleanQuery = new BooleanQuery();

        if (start != null && !start.isEmpty()) {
            BooleanQuery startQuery = new BooleanQuery();
            for (int i = 0; i < numClusteringColumns; i++) {
                BooleanQuery q = new BooleanQuery();
                for (int j = 0; j < i; j++) {
                    String name = names[j];
                    Object value = queryValue(start, j);
                    q.add(new MatchConditionBuilder(name, value).build().query(schema), MUST);
                }
                String name = names[i];
                Object value = queryValue(start, i);
                boolean include = (i == numClusteringColumns - 1) && includeStart(start);
                q.add(new RangeConditionBuilder(name).lower(value).includeLower(include).build().query(schema), MUST);
                startQuery.add(q, SHOULD);
            }
            booleanQuery.add(startQuery, MUST);
        }

        if (stop != null && !stop.isEmpty()) {
            BooleanQuery stopQuery = new BooleanQuery();
            for (int i = 0; i < numClusteringColumns; i++) {
                BooleanQuery q = new BooleanQuery();
                for (int j = 0; j < i; j++) {
                    String name = names[j];
                    Object value = queryValue(stop, j);
                    q.add(new MatchConditionBuilder(name, value).build().query(schema), MUST);
                }
                String name = names[i];
                Object value = queryValue(stop, i);
                boolean include = (i == numClusteringColumns - 1) && includeStop(stop);
                q.add(new RangeConditionBuilder(name).upper(value).includeUpper(include).build().query(schema), MUST);
                stopQuery.add(q, SHOULD);
            }
            booleanQuery.add(stopQuery, MUST);
        }

        return booleanQuery.getClauses().length == 0 ? null : booleanQuery;
    }
}
