package org.apache.cassandra.db.index.stratio.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.net.MessagingService;

/**
 * Class for serializing {@link ColumnFamily} from/to byte array.
 * 
 * @author adelapena
 * 
 */
public class ColumnFamilySerializer {

	public static final int VERSION = MessagingService.VERSION_20;

	private static org.apache.cassandra.db.ColumnFamilySerializer cfs = new org.apache.cassandra.db.ColumnFamilySerializer();

	/**
	 * Returns the {@code byte} array representation of the specified {@link ColumnFamily}.
	 * 
	 * @param columnFamily
	 *            The column family to be serialized.
	 * @return The {@code byte} array representation of the specified {@link ColumnFamily}.
	 */
	public static byte[] bytes(ColumnFamily columnFamily) {
		int size = (int) cfs.serializedSize(columnFamily, VERSION);
		ByteArrayOutputStream os = new ByteArrayOutputStream(size);
		DataOutputStream dos = new DataOutputStream(os);
		cfs.serialize(columnFamily, dos, VERSION);
		byte[] bytes = os.toByteArray();
		return bytes;
	}

	/**
	 * Returns the {@code ColumnFamily} representation of the specified {@code byte} array.
	 * 
	 * @param bytes
	 *            The {@code byte} array to be serialized.
	 * @return The {@code ColumnFamily} representation of the specified {@code byte} array.
	 * @throws IOException
	 */
	public static ColumnFamily columnFamily(byte[] bytes) throws IOException {
		InputStream is = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(is);
		return cfs.deserialize(dis, VERSION);
	}
}
