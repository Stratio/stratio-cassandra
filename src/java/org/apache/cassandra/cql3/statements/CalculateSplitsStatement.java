/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateSplitsStatement extends CFStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CalculateSplitsStatement.class);

    private final int boundTerms = 0;
    private final IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
    private final int thriftPort = DatabaseDescriptor.getRpcPort();
    private final TokenFactory<?> tokenFactory = partitioner.getTokenFactory();
    private final Integer splitSize;
    private final AbstractType tokenValidator;
    private final AbstractType<List<InetAddress>> locationsValidator;
    private final CFMetaData cfMetaData;
    private final List<ColumnSpecification> rsMetadata;

    String ks;
    String cf;

    public CalculateSplitsStatement(CFName name, Integer splitSize)
    {
        super(name);
        this.splitSize = splitSize;
        ks = cfName.getKeyspace();
        cf = cfName.getColumnFamily();

        tokenValidator = partitioner.getTokenValidator();
        locationsValidator = ListType.getInstance(InetAddressType.instance);

        rsMetadata = new ArrayList<>();
        rsMetadata.add(new ColumnSpecification(ks, cf, new ColumnIdentifier("start_token", true), tokenValidator));
        rsMetadata.add(new ColumnSpecification(ks, cf, new ColumnIdentifier("end_token", true), tokenValidator));
        rsMetadata.add(new ColumnSpecification(ks, cf, new ColumnIdentifier("preferred_locations", true), locationsValidator));
        rsMetadata.add(new ColumnSpecification(ks, cf, new ColumnIdentifier("estimated_rows", true), LongType.instance));

        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cf);
        cfMetaData = cfs.metadata;
    }

    /**
     * Returns the number of bound terms in this statement.
     */
    public int getBoundTerms()
    {
        return boundTerms;
    }

    /**
     * Perform any access verification necessary for the statement.
     * 
     * @param state
     *            the current client state
     */
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
    }

    /**
     * Perform additional validation required by the statment. To be overriden by subclasses if needed.
     * 
     * @param state
     *            the current client state
     */
    public void validate(ClientState state) throws RequestValidationException
    {

    }

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     * 
     * @param state
     *            the current query state
     * @param options
     *            options for this query (consistency, variables, pageSize, ...)
     */
    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException,
            RequestExecutionException
    {

        ResultSet rset = new ResultSet(rsMetadata);

        Map<Range<Token>, List<InetAddress>> ranges = StorageService.instance.getRangeToAddressMapInLocalDC(ks);
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : ranges.entrySet())
        {

            Range<Token> range = entry.getKey();
            List<InetAddress> endPoints = entry.getValue();

            for (Pair<Range<Token<?>>, Long> split : split(range, endPoints, splitSize))
            {
                Range<Token<?>> splitRange = split.left;
                List<ByteBuffer> columns = new ArrayList<>();
                columns.add(tokenValidator.decompose(splitRange.left.token));
                columns.add(tokenValidator.decompose(splitRange.right.token));
                columns.add(locationsValidator.decompose(endPoints));
                columns.add(tokenValidator.decompose(split.right));
                rset.addRow(columns);
            }
        }

        return new ResultMessage.Rows(rset);
    }

    private List<Pair<Range<Token<?>>, Long>> split(Range<Token> range, List<InetAddress> endPoints, int splitSize)
            throws RequestValidationException
    {

        String splitStart = tokenFactory.toString(range.left.getToken());
        String splitEnd = tokenFactory.toString(range.right.getToken());

        List<Pair<Range<Token<?>>, Long>> splits = new ArrayList<>();
        for (InetAddress endPoint : endPoints)
        {
            try
            {
                String host = endPoint.getHostName();
                TTransport transport = new TFramedTransport(new TSocket(host, thriftPort));
                TProtocol protocol = new TBinaryProtocol(transport);
                Cassandra.Client client = new Cassandra.Client(protocol);
                transport.open();
                client.set_keyspace(ks);
                List<CfSplit> cfSplits = client.describe_splits_ex(cf, splitStart, splitEnd, splitSize);
                for (CfSplit cfSplit : cfSplits)
                {
                    Token<?> left = tokenFactory.fromString(cfSplit.start_token);
                    Token<?> right = tokenFactory.fromString(cfSplit.end_token);
                    Range<Token<?>> subRange = new Range<>(left, right);
                    Pair<Range<Token<?>>, Long> split = Pair.create(subRange, cfSplit.row_count);
                    splits.add(split);
                }
                transport.close();
                return splits;
            }
            catch (TException e)
            {
                e.printStackTrace();
            }
        }
        throw new InvalidRequestException("No replicas available for range " + range);
    }

    /**
     * Variante of execute used for internal query against the system tables, and thus only query the local node.
     * 
     * @param state
     *            the current query state
     */
    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException,
            RequestExecutionException
    {
        return execute(state, options);
    }

    public Prepared prepare() throws RequestValidationException
    {
        CalculateSplitsStatement stmt = new CalculateSplitsStatement(cfName, splitSize);
        return new ParsedStatement.Prepared(stmt);
    }
}
