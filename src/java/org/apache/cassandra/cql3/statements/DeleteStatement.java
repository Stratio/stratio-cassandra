/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.InvalidRequestException;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;
import static org.apache.cassandra.cql.QueryProcessor.validateColumnName;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 *
 */
public class DeleteStatement extends AbstractModification
{
    private final List<ColumnIdentifier> columns;
    private final List<Relation> whereClause;

    private final Map<ColumnIdentifier, List<Term>> processedKeys = new HashMap<ColumnIdentifier, List<Term>>();

    public DeleteStatement(CFName name, List<ColumnIdentifier> columns, List<Relation> whereClause, Attributes attrs)
    {
        super(name, attrs);

        this.columns = columns;
        this.whereClause = whereClause;
    }

    public List<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables) throws InvalidRequestException
    {
        clientState.hasColumnFamilyAccess(columnFamily(), Permission.WRITE);
        CFMetaData metadata = validateColumnFamily(keyspace(), columnFamily());
        CFDefinition cfDef = metadata.getCfDef();

        preprocess(cfDef);

        // Check key
        List<Term> keys = processedKeys.get(cfDef.key.name);
        if (keys == null || keys.isEmpty())
            throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", cfDef.key.name));

        CompositeType.Builder builder = null;
        if (cfDef.isComposite())
        {
            builder = new CompositeType.Builder((CompositeType)metadata.comparator);
            CFDefinition.Name firstEmpty = null;
            for (CFDefinition.Name name : cfDef.columns.values())
            {
                List<Term> values = processedKeys.get(name.name);
                if (values == null || values.isEmpty())
                {
                    firstEmpty = name;
                    if (cfDef.kind == CFDefinition.Kind.SPARSE && builder.componentCount() != 0)
                        throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));
                }
                else if (firstEmpty != null)
                {
                    throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmpty, name));
                }
                else
                {
                    assert values.size() == 1; // We only allow IN for keys so far
                    builder.add(values.get(0), Relation.Type.EQ, variables);
                }
            }
        }

        List<IMutation> rowMutations = new ArrayList<IMutation>();

        for (Term key : keys)
        {
            ByteBuffer rawKey = key.getByteBuffer(cfDef.key.type, variables);
            rowMutations.add(mutationForKey(cfDef, clientState, rawKey, builder, variables));
        }

        return rowMutations;
    }

    public RowMutation mutationForKey(CFDefinition cfDef, ClientState clientState, ByteBuffer key, CompositeType.Builder builder, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        RowMutation rm = new RowMutation(cfDef.cfm.ksName, key);

        if (columns.isEmpty()
           && (builder == null || builder.componentCount() == 0)
           && (cfDef.kind != CFDefinition.Kind.DYNAMIC || processedKeys.get(cfDef.columnNameForDynamic().name) == null))
        {
            // No columns, delete the row
            rm.delete(new QueryPath(columnFamily()), getTimestamp(clientState));
        }
        else
        {
            for (ColumnIdentifier column : columns)
            {
                CFDefinition.Name name = cfDef.get(column);
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", column));

                // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
                // list. However, we support having the value name for coherence with the static/sparse case
                if (name.kind != CFDefinition.Name.Kind.COLUMN_METADATA && name.kind != CFDefinition.Name.Kind.VALUE_ALIAS)
                    throw new InvalidRequestException(String.format("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", column));
            }

            switch (cfDef.kind)
            {
                case STATIC:
                case SPARSE:
                    // Delete specific columns
                    for (ColumnIdentifier column : columns)
                    {
                        ByteBuffer columnName = builder == null ? column.key : builder.copy().add(column.key).build();
                        validateColumnName(columnName);
                        rm.delete(new QueryPath(columnFamily(), null, columnName), getTimestamp(clientState));
                    }
                    break;
                case DYNAMIC:
                    CFDefinition.Name name = cfDef.columnNameForDynamic();
                    List<Term> colValues = processedKeys.get(name.name);
                    assert colValues != null && colValues.size() == 1;
                    ByteBuffer columnName = colValues.get(0).getByteBuffer(name.type, variables);
                    validateColumnName(columnName);
                    rm.delete(new QueryPath(columnFamily(), null, columnName), getTimestamp(clientState));
                    break;
                case DENSE:
                    rm.delete(new QueryPath(columnFamily(), null, builder.build()), getTimestamp(clientState));
                    break;
            }
        }

        return rm;
    }

    private void preprocess(CFDefinition cfDef) throws InvalidRequestException
    {
        UpdateStatement.processKeys(cfDef, whereClause, processedKeys);
    }

    public String toString()
    {
        return String.format("DeleteStatement(name=%s, columns=%s, consistency=%s keys=%s)",
                             cfName,
                             columns,
                             cLevel,
                             whereClause);
    }
}
