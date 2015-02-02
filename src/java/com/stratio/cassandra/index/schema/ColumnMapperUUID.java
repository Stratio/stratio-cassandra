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

import com.google.common.primitives.Longs;
import com.stratio.cassandra.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * A {@link ColumnMapper} to map a UUID field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperUUID extends ColumnMapperSingle<String> {

    /**
     * Builds a new {@link ColumnMapperUUID}.
     */
    @JsonCreator
    public ColumnMapperUUID() {
        super(new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance, UUIDType.instance, TimeUUIDType.instance},
              new AbstractType[]{UUIDType.instance, TimeUUIDType.instance});
    }

    /** {@inheritDoc} */
    @Override
    public Analyzer analyzer() {
        return EMPTY_ANALYZER;
    }

    /** {@inheritDoc} */
    @Override
    public String indexValue(String name, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof UUID) {
            UUID uuid = (UUID) value;
            return serialize(uuid);
        } else if (value instanceof String) {
            UUID uuid = UUID.fromString((String) value);
            return serialize(uuid);
        } else {
            throw new IllegalArgumentException();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String queryValue(String name, Object value) {
        return indexValue(name, value);
    }

    /** {@inheritDoc} */
    @Override
    public Field field(String name, Object value) {
        String uuid = indexValue(name, value);
        return new StringField(name, uuid, STORE);
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String field, boolean reverse) {
        return new SortField(field, Type.STRING, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Class<String> baseClass() {
        return String.class;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new ToStringBuilder(this).toString();
    }

    /**
     * Returns the {@link String} representation of the specified {@link UUID}. The returned value has the same
     * collation as {@link UUIDType}.
     *
     * @param uuid The {@link UUID} to be serialized.
     * @return The {@link String} representation of the specified {@link UUID}.
     */
    public static String serialize(UUID uuid) {

        StringBuilder sb = new StringBuilder();

        // Get UUID type version
        ByteBuffer bb = UUIDType.instance.decompose(uuid);
        int version = (bb.get(bb.position() + 6) >> 4) & 0x0f;

        // Add version at the beginning
        sb.append(ByteBufferUtils.toHex((byte) version));

        // If it's a time based UUID, add the UNIX timestamp
        if (version == 1) {
            long timestamp = uuid.timestamp();
            String timestampHex = ByteBufferUtils.toHex(Longs.toByteArray(timestamp));
            sb.append(timestampHex);
        }

        // Add the UUID itself
        sb.append(ByteBufferUtils.toHex(bb));
        return sb.toString();
    }

}
