package org.apache.cassandra.db.index.stratio.util;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Class for mapping several binary types from/to {@link String} using a base of 256 UTF-8
 * characters.
 * 
 * @author adelapena
 * 
 */
public class Base256Serializer {

	/**
	 * Returns the {@code char} array representation of the specified {@code byte} array.
	 * 
	 * @param bytes
	 *            The {@code byte} array to be converted.
	 * @return The {@code char} array representation of the specified {@code byte} array.
	 */
	public static char[] chars(byte[] bytes) {
		char[] chars = new char[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			int pos = bytes[i] & 0xff;
			chars[i] = (char) pos;
		}
		return chars;
	}

	/**
	 * Returns the {@code byte} array representation of the specified {@code char} array.
	 * 
	 * @param chars
	 *            The {@code char} array to be converted.
	 * @return The {@code byte} array representation of the specified {@code char} array.
	 */
	public static byte[] bytes(char[] chars) {
		byte[] bytes = new byte[chars.length];
		for (int i = 0; i < bytes.length; i++) {
			char c = chars[i];
			bytes[i] = (byte) c;
		}
		return bytes;
	}

	/**
	 * Returns the {@code byte} array representation of the specified {@code String}.
	 * 
	 * @param string
	 *            The {@code String} to be converted.
	 * @return The {@code byte} array representation of the specified {@code String}.
	 */
	public static byte[] bytes(String string) {
		return bytes(string.toCharArray());
	}

	/**
	 * Returns the {@code String} representation of the specified {@code byte} array.
	 * 
	 * @param bytes
	 *            The {@code byte} array to be converted.
	 * @return The {@code String} representation of the specified {@code byte} array.
	 */
	public static String string(byte[] bytes) {
		return new String(chars(bytes));
	}

	/**
	 * Returns the {@code String} representation of the specified {@code ByteBuffer}.
	 * 
	 * @param byteBuffer
	 *            The {@code ByteBuffer} to be converted.
	 * @return The {@code String} representation of the specified {@code ByteBuffer}.
	 */
	public static String string(ByteBuffer byteBuffer) {
		ByteBuffer bb = ByteBufferUtil.clone(byteBuffer);
		byte[] bytes = new byte[bb.remaining()];
		bb.get(bytes);
		return new String(chars(bytes));
	}

	/**
	 * Returns the {@code ByteBuffer} representation of the specified {@code String}.
	 * 
	 * @param string
	 *            The {@code String} to be converted.
	 * @return The {@code ByteBuffer} representation of the specified {@code String}.
	 */
	public static ByteBuffer byteBuffer(String string) {
		return ByteBuffer.wrap(bytes(string));
	}
}
