package org.apache.cassandra.db.index.stratio.util;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log {

	protected static final Logger log = LoggerFactory.getLogger("stratio");

	public static void info(String message, Object... options) {
		log.info(String.format(message, format(options)));
	}

	public static void info(Throwable throwable, String message, Object... options) {
		log.info(String.format(message, format(options)), throwable);
	}

	public static void debug(String message, Object... options) {
		log.debug(String.format(message, format(options)));
	}

	public static void debug(Throwable throwable, String message, Object... options) {
		log.debug(String.format(message, format(options)), throwable);
	}

	public static void error(String message, Object... options) {
		log.error(String.format(message, format(options)));
	}

	public static void error(Throwable throwable, String message, Object... options) {
		log.error(String.format(message, format(options)), throwable);
	}

	public static void warn(String message, Object... options) {
		log.warn(String.format(message, format(options)));
	}

	public static void warn(Throwable throwable, String message, Object... options) {
		log.warn(String.format(message, format(options)), throwable);
	}

	private static Object[] format(Object... options) {
		Object[] result = new Object[options.length];
		for (int i = 0; i < options.length; i++) {
			Object option = options[i];
			if (option instanceof ByteBuffer)
				option = ByteBufferUtils.toHex((ByteBuffer) option);
			result[i] = option;
		}
		return result;
	}

}
