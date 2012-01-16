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
package org.apache.cassandra.cql3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.migration.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import org.antlr.runtime.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class QueryProcessor
{
    public static final String CQL_VERSION = "3.0.0";

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    private static final long timeLimitForSchemaAgreement = 10 * 1000;

    private static List<org.apache.cassandra.db.Row> getSlice(SelectStatement select, List<ByteBuffer> variables)
    throws InvalidRequestException, TimedOutException, UnavailableException
    {
        QueryPath queryPath = new QueryPath(select.columnFamily());
        List<ReadCommand> commands = new ArrayList<ReadCommand>();

        // ...of a list of column names
        if (!select.isColumnRange())
        {
            Collection<ByteBuffer> columnNames = select.getRequestedColumns(variables);
            validateColumnNames(columnNames);

            for (ByteBuffer key: select.getKeys(variables))
            {
                validateKey(key);
                commands.add(new SliceByNamesReadCommand(select.keyspace(), key, queryPath, columnNames));
            }
        }
        // ...a range (slice) of column names
        else
        {
            ByteBuffer start = select.getRequestedStart(variables);
            ByteBuffer finish = select.getRequestedFinish(variables);

            // Note that we use the total limit for every key. This is
            // potentially inefficient, but then again, IN + LIMIT is not a
            // very sensible choice
            for (ByteBuffer key : select.getKeys(variables))
            {
                validateKey(key);
                validateSliceRange(select.cfDef.cfm, start, finish, select.isColumnsReversed());
                commands.add(new SliceFromReadCommand(select.keyspace(),
                                                      key,
                                                      queryPath,
                                                      start,
                                                      finish,
                                                      select.isColumnsReversed(),
                                                      select.getLimit()));
            }
        }

        try
        {
            return StorageProxy.read(commands, select.getConsistencyLevel());
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static List<org.apache.cassandra.db.Row> multiRangeSlice(SelectStatement select, List<ByteBuffer> variables)
    throws TimedOutException, UnavailableException, InvalidRequestException
    {
        List<org.apache.cassandra.db.Row> rows;
        IPartitioner<?> p = StorageService.getPartitioner();

        ByteBuffer startKeyBytes = select.getKeyStart(variables);
        ByteBuffer finishKeyBytes = select.getKeyFinish(variables);

        RowPosition startKey = RowPosition.forKey(startKeyBytes, p), finishKey = RowPosition.forKey(finishKeyBytes, p);
        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
        {
            if (p instanceof RandomPartitioner)
                throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
            else
                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
        }
        AbstractBounds<RowPosition> bounds;
        if (select.includeStartKey())
        {
            bounds = select.includeFinishKey()
                   ? new Bounds<RowPosition>(startKey, finishKey)
                   : new IncludingExcludingBounds<RowPosition>(startKey, finishKey);
        }
        else
        {
            bounds = select.includeFinishKey()
                   ? new Range<RowPosition>(startKey, finishKey)
                   : new ExcludingBounds<RowPosition>(startKey, finishKey);
        }

        // XXX: Our use of Thrift structs internally makes me Sad. :(
        SlicePredicate thriftSlicePredicate = slicePredicateFromSelect(select, variables);
        validateSlicePredicate(select.cfDef.cfm, thriftSlicePredicate);

        List<IndexExpression> expressions = select.getIndexExpressions(variables);

        try
        {
            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(select.keyspace(),
                                                                    select.columnFamily(),
                                                                    null,
                                                                    thriftSlicePredicate,
                                                                    bounds,
                                                                    expressions,
                                                                    select.getLimit(),
                                                                    true), // limit by columns, not keys
                                                                    select.getConsistencyLevel());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            throw new UnavailableException();
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        return rows;
    }

    private static void batchUpdate(ClientState state, AbstractModification statement, List<ByteBuffer> variables)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        List<IMutation> rowMutations = new ArrayList<IMutation>();
        statement.validate(state);

        try
        {
            StorageProxy.mutate(statement.getMutations(state, variables), statement.getConsistencyLevel());
        }
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            throw new UnavailableException();
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
    }

    private static SlicePredicate slicePredicateFromSelect(SelectStatement select, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        SlicePredicate thriftSlicePredicate = new SlicePredicate();

        if (select.isColumnRange())
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.start = select.getRequestedStart(variables);
            sliceRange.finish = select.getRequestedFinish(variables);
            sliceRange.reversed = select.isColumnsReversed();
            sliceRange.count = 1; // We use this for range slices, where the count is ignored in favor of the global column count
            thriftSlicePredicate.slice_range = sliceRange;
        }
        else
        {
            thriftSlicePredicate.column_names = select.getRequestedColumns(variables);
        }
        return thriftSlicePredicate;
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    private static void validateColumnNames(Iterable<ByteBuffer> columns)
    throws InvalidRequestException
    {
        for (ByteBuffer name : columns)
        {
            if (name.remaining() > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException(String.format("column name is too long (%s > %s)",
                                                                name.remaining(),
                                                                IColumn.MAX_NAME_LENGTH));
            if (name.remaining() == 0)
                throw new InvalidRequestException("zero-length column name");
        }
    }

    public static void validateColumnName(ByteBuffer column)
    throws InvalidRequestException
    {
        validateColumnNames(Collections.singletonList(column));
    }

    private static void validateSlicePredicate(CFMetaData metadata, SlicePredicate predicate)
    throws InvalidRequestException
    {
        if (predicate.slice_range != null)
            validateSliceRange(metadata, predicate.slice_range);
        else
            validateColumnNames(predicate.column_names);
    }

    private static void validateSliceRange(CFMetaData metadata, SliceRange range)
    throws InvalidRequestException
    {
        validateSliceRange(metadata, range.start, range.finish, range.reversed);
    }

    private static void validateSliceRange(CFMetaData metadata, ByteBuffer start, ByteBuffer finish, boolean reversed)
    throws InvalidRequestException
    {
        AbstractType<?> comparator = metadata.getComparatorFor(null);
        Comparator<ByteBuffer> orderedComparator = reversed ? comparator.reverseComparator: comparator;
        if (start.remaining() > 0 && finish.remaining() > 0 && orderedComparator.compare(start, finish) > 0)
            throw new InvalidRequestException("range finish must come after start in traversal order");
    }

    // Copypasta from CassandraServer (where it is private).
    private static void validateSchemaAgreement() throws SchemaDisagreementException
    {
       if (describeSchemaVersions().size() > 1)
            throw new SchemaDisagreementException();
    }

    // Copypasta from o.a.c.thrift.CassandraDaemon
    private static void applyMigrationOnStage(final Migration m) throws SchemaDisagreementException, InvalidRequestException
    {
        Future<?> f = StageManager.getStage(Stage.MIGRATION).submit(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                m.apply();
                m.announce();
                return null;
            }
        });
        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            // this means call() threw an exception. deal with it directly.
            if (e.getCause() != null)
            {
                InvalidRequestException ex = new InvalidRequestException(e.getCause().getMessage());
                ex.initCause(e.getCause());
                throw ex;
            }
            else
            {
                InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                ex.initCause(e);
                throw ex;
            }
        }

        validateSchemaIsSettled();
    }

    private static Map<String, List<String>> describeSchemaVersions()
    {
        // unreachable hosts don't count towards disagreement
        return Maps.filterKeys(StorageProxy.describeSchemaVersions(),
                               Predicates.not(Predicates.equalTo(StorageProxy.UNREACHABLE)));
    }

    private static CqlResult processStatement(CQLStatement statement,ClientState clientState, List<ByteBuffer> variables)
    throws  UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        CqlResult result = new CqlResult();

        if (logger.isDebugEnabled()) logger.debug("CQL statement type: {}", statement.type.toString());
        CFMetaData metadata;
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;
                clientState.hasColumnFamilyAccess(select.keyspace(), select.columnFamily(), Permission.READ);

                List<org.apache.cassandra.db.Row> rows;

                // By-key
                if (!select.isKeyRange())
                {
                    rows = getSlice(select, variables);
                }
                else
                {
                    rows = multiRangeSlice(select, variables);
                }

                result.type = CqlResultType.ROWS;
                select.processResult(rows, result);
                return result;

            case INSERT: // insert uses UpdateStatement
            case UPDATE:
            case BATCH:
            case DELETE:
                AbstractModification update = (AbstractModification)statement.statement;
                batchUpdate(clientState, update, variables);
                result.type = CqlResultType.VOID;
                return result;

            case USE:
                clientState.setKeyspace((String) statement.statement);
                result.type = CqlResultType.VOID;

                return result;

            case TRUNCATE:
                TruncateStatement truncate = (TruncateStatement)statement.statement;

                validateColumnFamily(truncate.keyspace(), truncate.columnFamily());
                clientState.hasColumnFamilyAccess(truncate.keyspace(), truncate.columnFamily(), Permission.WRITE);

                try
                {
                    StorageProxy.truncateBlocking(truncate.keyspace(), truncate.columnFamily());
                }
                catch (TimeoutException e)
                {
                    throw (UnavailableException) new UnavailableException().initCause(e);
                }
                catch (IOException e)
                {
                    throw (UnavailableException) new UnavailableException().initCause(e);
                }

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_KEYSPACE:
                CreateKeyspaceStatement create = (CreateKeyspaceStatement)statement.statement;
                create.validate();
                clientState.hasKeyspaceSchemaAccess(Permission.WRITE);
                validateSchemaAgreement();

                try
                {
                    KsDef ksd = new KsDef(create.keyspace(),
                                          create.getStrategyClass(),
                                          Collections.<CfDef>emptyList())
                                .setStrategy_options(create.getStrategyOptions());
                    ThriftValidation.validateKsDef(ksd);
                    ThriftValidation.validateKeyspaceNotYetExisting(create.keyspace());
                    applyMigrationOnStage(new AddKeyspace(KSMetaData.fromThrift(ksd)));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_COLUMNFAMILY:
                CreateColumnFamilyStatement createCf = (CreateColumnFamilyStatement)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();
                CFMetaData cfmd = createCf.getCFMetaData();
                ThriftValidation.validateCfDef(cfmd.toThrift(), null);

                try
                {
                    applyMigrationOnStage(new AddColumnFamily(cfmd));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_INDEX:
                CreateIndexStatement createIdx = (CreateIndexStatement)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();
                CFMetaData oldCfm = Schema.instance.getCFMetaData(createIdx.keyspace(), createIdx.columnFamily());
                if (oldCfm == null)
                    throw new InvalidRequestException("No such column family: " + createIdx.columnFamily());

                boolean columnExists = false;
                ColumnIdentifier columnName = createIdx.getColumnName();
                // mutating oldCfm directly would be bad, but mutating a Thrift copy is fine.  This also
                // sets us up to use validateCfDef to check for index name collisions.
                CfDef cf_def = oldCfm.toThrift();
                for (ColumnDef cd : cf_def.column_metadata)
                {
                    if (cd.name.equals(columnName.key))
                    {
                        if (cd.index_type != null)
                            throw new InvalidRequestException("Index already exists");
                        if (logger.isDebugEnabled())
                            logger.debug("Updating column {} definition for index {}", columnName, createIdx.getIndexName());
                        cd.setIndex_type(IndexType.KEYS);
                        cd.setIndex_name(createIdx.getIndexName());
                        columnExists = true;
                        break;
                    }
                }
                if (!columnExists)
                {
                    CFDefinition cfDef = oldCfm.getCfDef();
                    CFDefinition.Name name = cfDef.get(columnName);
                    if (name != null)
                    {
                        switch (name.kind)
                        {
                            case KEY_ALIAS:
                            case COLUMN_ALIAS:
                                throw new InvalidRequestException(String.format("Cannot create index on PRIMARY KEY part %s", columnName));
                            case VALUE_ALIAS:
                                throw new InvalidRequestException(String.format("Cannot create index on column %s of compact CF", columnName));
                        }
                    }
                    throw new InvalidRequestException("No column definition found for column " + columnName);
                }

                CFMetaData.addDefaultIndexNames(cf_def);
                ThriftValidation.validateCfDef(cf_def, oldCfm);
                try
                {
                    org.apache.cassandra.db.migration.avro.CfDef result1;
                    try
                    {
                        result1 = CFMetaData.fromThrift(cf_def).toAvro();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                    applyMigrationOnStage(new UpdateColumnFamily(result1));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_INDEX:
                DropIndexStatement dropIdx = (DropIndexStatement)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();

                try
                {
                    applyMigrationOnStage(dropIdx.generateMutation());
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_KEYSPACE:
                String deleteKeyspace = (String)statement.statement;
                clientState.hasKeyspaceSchemaAccess(Permission.WRITE);
                validateSchemaAgreement();

                try
                {
                    applyMigrationOnStage(new DropKeyspace(deleteKeyspace));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_COLUMNFAMILY:
                DropColumnFamilyStatement drop = (DropColumnFamilyStatement)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();

                try
                {
                    applyMigrationOnStage(new DropColumnFamily(drop.keyspace(), drop.columnFamily()));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case ALTER_TABLE:
                AlterTableStatement alterTable = (AlterTableStatement) statement.statement;

                validateColumnFamily(alterTable.keyspace(), alterTable.columnFamily());
                clientState.hasColumnFamilyAccess(alterTable.columnFamily(), Permission.WRITE);
                validateSchemaAgreement();

                try
                {
                    applyMigrationOnStage(new UpdateColumnFamily(alterTable.getCfDef()));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;
        }
        return null;    // We should never get here.
    }

    public static CqlResult process(String queryString, ClientState clientState)
    throws RecognitionException, UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        logger.trace("CQL QUERY: {}", queryString);
        return processStatement(getStatement(queryString, clientState), clientState, Collections.<ByteBuffer>emptyList());
    }

    public static CqlPreparedResult prepare(String queryString, ClientState clientState)
    throws RecognitionException, InvalidRequestException
    {
        logger.trace("CQL QUERY: {}", queryString);

        CQLStatement statement = getStatement(queryString, clientState);
        int statementId = makeStatementId(queryString);
        logger.trace("Discovered "+ statement.boundTerms + " bound variables.");

        clientState.getCQL3Prepared().put(statementId, statement);
        logger.trace(String.format("Stored prepared statement #%d with %d bind markers",
                                   statementId,
                                   statement.boundTerms));

        return new CqlPreparedResult(statementId, statement.boundTerms);
    }

    public static CqlResult processPrepared(CQLStatement statement, ClientState clientState, List<ByteBuffer> variables)
    throws UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.boundTerms == 0)))
        {
            if (variables.size() != statement.boundTerms)
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.boundTerms,
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        return processStatement(statement, clientState, variables);
    }

    private static final int makeStatementId(String cql)
    {
        // use the hash of the string till something better is provided
        return cql.hashCode();
    }

    private static CQLStatement getStatement(String queryStr, ClientState clientState) throws InvalidRequestException, RecognitionException
    {
        CQLStatement statement = parseStatement(queryStr);

        // Set keyspace for statement that require login
        if (statement.statement instanceof CFStatement)
            ((CFStatement)statement.statement).prepareKeyspace(clientState);

        if (statement.statement instanceof Preprocessable)
            statement = new CQLStatement(statement.type, ((Preprocessable)statement.statement).preprocess(), statement.boundTerms);

        return statement;
    }

    private static CQLStatement parseStatement(String queryStr) throws InvalidRequestException, RecognitionException
    {
        // Lexer and parser
        CharStream stream = new ANTLRStringStream(queryStr);
        CqlLexer lexer = new CqlLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);

        // Parse the query string to a statement instance
        CQLStatement statement = parser.query();

        // The lexer and parser queue up any errors they may have encountered
        // along the way, if necessary, we turn them into exceptions here.
        lexer.throwLastRecognitionError();
        parser.throwLastRecognitionError();

        return statement;
    }

    private static void validateSchemaIsSettled() throws SchemaDisagreementException
    {
        long limit = System.currentTimeMillis() + timeLimitForSchemaAgreement;

        outer:
        while (limit - System.currentTimeMillis() >= 0)
        {
            String currentVersionId = Schema.instance.getVersion().toString();
            for (String version : describeSchemaVersions().keySet())
            {
                if (!version.equals(currentVersionId))
                    continue outer;
            }

            // schemas agree
            return;
        }

        throw new SchemaDisagreementException();
    }
}
