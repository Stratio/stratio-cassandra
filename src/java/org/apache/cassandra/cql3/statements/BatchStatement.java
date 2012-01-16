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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.RequestType;
import org.apache.cassandra.thrift.ThriftValidation;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
public class BatchStatement extends AbstractModification
{
    // statements to execute
    protected final List<AbstractModification> statements;

    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
     *
     * @param statements a list of UpdateStatements
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public BatchStatement(List<AbstractModification> statements, Attributes attrs)
    {
        super(null, attrs);
        this.statements = statements;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        for (AbstractModification statement : statements)
            statement.prepareKeyspace(state);
    }

    @Override
    public void validate(ClientState state) throws InvalidRequestException
    {
        if (getTimeToLive() != 0)
            throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

        Set<String> cfamsSeen = new HashSet<String>();
        for (AbstractModification statement : statements)
        {
            if (statement.isSetConsistencyLevel())
                throw new InvalidRequestException("Consistency level must be set on the BATCH, not individual statements");

            if (isSetTimestamp() && statement.isSetTimestamp())
                throw new InvalidRequestException("Timestamp must be set either on BATCH or individual statements");

            if (statement.getTimeToLive() < 0)
                throw new InvalidRequestException("A TTL must be greater or equal to 0");

            ThriftValidation.validateConsistencyLevel(statement.keyspace(), getConsistencyLevel(), RequestType.WRITE);

            // Avoid unnecessary authorizations.
            if (!(cfamsSeen.contains(statement.columnFamily())))
            {
                state.hasColumnFamilyAccess(statement.keyspace(), statement.columnFamily(), Permission.WRITE);
                cfamsSeen.add(statement.columnFamily());
            }
        }
    }

    public List<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        List<IMutation> batch = new LinkedList<IMutation>();

        for (AbstractModification statement : statements)
        {
            if (isSetTimestamp())
                statement.timestamp = timestamp;
            batch.addAll(statement.getMutations(clientState, variables));
        }

        return batch;
    }

    public String toString()
    {
        return String.format("BatchStatement(statements=%s, consistency=%s)", statements, cLevel);
    }
}
