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
package com.stratio.cassandra.index.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;

/**
 * Class for serializing {@link ColumnFamily} from/to byte array.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class ColumnFamilySerializer
{

    public static final int VERSION = MessagingService.VERSION_20;

    private static org.apache.cassandra.db.ColumnFamilySerializer cfs = new org.apache.cassandra.db.ColumnFamilySerializer();

    /**
     * Returns the {@code byte} array representation of the specified {@link ColumnFamily}.
     * 
     * @param columnFamily
     *            The column family to be serialized.
     * @return The {@code byte} array representation of the specified {@link ColumnFamily}.
     */
    public static byte[] bytes(ColumnFamily columnFamily)
    {
        int size = (int) cfs.serializedSize(columnFamily, VERSION);
        ByteArrayOutputStream os = new ByteArrayOutputStream(size);
        DataOutputStreamPlus dos = new DataOutputStreamPlus(os);
        cfs.serialize(columnFamily, dos, VERSION);
        return os.toByteArray();
    }

    /**
     * Returns the {@code ColumnFamily} representation of the specified {@code byte} array.
     * 
     * @param bytes
     *            The {@code byte} array to be serialized.
     * @return The {@code ColumnFamily} representation of the specified {@code byte} array.
     * @throws IOException
     */
    public static ColumnFamily columnFamily(byte[] bytes) throws IOException
    {
        InputStream is = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(is);
        return cfs.deserialize(dis, VERSION);
    }
}
