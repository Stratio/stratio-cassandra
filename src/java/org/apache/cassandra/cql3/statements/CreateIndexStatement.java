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

import org.apache.cassandra.cql3.*;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends CFStatement
{
    private final String indexName;
    private final ColumnIdentifier columnName;

    public CreateIndexStatement(CFName name, String indexName, ColumnIdentifier columnName)
    {
        super(name);
        this.indexName = indexName;
        this.columnName = columnName;
    }

    /** Column name to index. */
    public ColumnIdentifier getColumnName()
    {
        return columnName;
    }

    /** Index name (or null). */
    public String getIndexName()
    {
        return indexName;
    }
}
