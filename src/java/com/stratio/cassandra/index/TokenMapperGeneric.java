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
package com.stratio.cassandra.index;

import com.stratio.cassandra.index.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link TokenMapper} to be used when any {@link IPartitioner} when there is not a more specific implementation. It
 * indexes the token raw binary value as a Lucene's string field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class TokenMapperGeneric extends TokenMapper
{
    /**
     * The Lucene's field name.
     */
    public static final String FIELD_NAME = "_token_generic";

    /**
     * The partioner's token factory.
     */
    private final TokenFactory<?> factory;

    /**
     * Returns a new {@link TokenMapperGeneric}.
     */
    public TokenMapperGeneric(CFMetaData metadata)
    {
        super(metadata);
        factory = DatabaseDescriptor.getPartitioner().getTokenFactory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void addFields(Document document, DecoratedKey partitionKey)
    {
        ByteBuffer bb = factory.toByteArray(partitionKey.getToken());
        String serialized = ByteBufferUtils.toString(bb);
        Field field = new StringField(FIELD_NAME, serialized, Store.YES);
        document.add(field);
    }

    public BytesRef bytesRef(Token token) {
        ByteBuffer bb = factory.toByteArray(token);
        byte[] bytes = ByteBufferUtils.asArray(bb);
        return new BytesRef(bytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(DataRange dataRange)
    {
        return new TokenDataRangeQuery(FIELD_NAME, dataRange, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortField[] sortFields()
    {
        return new SortField[]{
                new SortField(FIELD_NAME, new FieldComparatorSource()
                {
                    @Override
                    public FieldComparator<?>
                    newComparator(String field, int hits, int sort, boolean reversed) throws IOException
                    {
                        return new TokenSorter(TokenMapperGeneric.this, hits, field);
                    }
                })};
    }

    /**
     * Returns the Cassanda's {@link Token} represented by the specified Lucene's {@link BytesRef}.
     *
     * @param bytesRef A Lucene's {@link BytesRef} representation of a Cassanda's {@link Token}.
     * @return the Cassanda's {@link Token} represented by the specified Lucene's {@link BytesRef}.
     */
    Token<?> token(BytesRef bytesRef)
    {
        String string = bytesRef.utf8ToString();
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return factory.fromByteArray(bb);
    }

}
