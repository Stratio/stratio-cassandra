package org.apache.cassandra.db.index.stratio;


/**
 * Exception thrown when {@link RowService} mapping errors are found.
 * 
 * @version 0.1
 * @author adelapena
 */
public class MappingException extends RuntimeException {

	public static final long serialVersionUID = 245243564364356L;

	/**
	 * Constructs a new {@link RowService} exception with {@code null} as its detail message. The cause
	 * is not initialized, and may subsequently be initialized by a call to {@link #initCause} .
	 */
	public MappingException() {
		super();
	}

	/**
	 * Constructs a new {@link RowService} exception with the specified detail message and cause.
	 * <p>
	 * Note that the detail message associated with {@code cause} is <i>not</i> automatically
	 * incorporated in this {@link RowService} exception's detail message.
	 * 
	 * @param cause
	 *            the cause (which is saved for later retrieval by the {@link #getCause()} method).
	 *            (A <tt>null</tt> value is permitted, and indicates that the cause is nonexistent
	 *            or unknown.)
	 * @param messageFormat
	 *            the detail {@code String} message format.
	 * @param messageArgs
	 *            arguments referenced by the message format specifiers in the format string. If
	 *            there are more arguments than format specifiers, the extra arguments are ignored.
	 *            The number of arguments is variable and may be zero.
	 */
	public MappingException(Throwable cause, String messageFormat, Object... messageArgs) {
		super(String.format(messageFormat, messageArgs), cause);
	}

	/**
	 * Constructs a new {@link RowService} exception with the specified detail message. The cause is not
	 * initialized, and may subsequently be initialized by a call to {@link #initCause}.
	 * 
	 * @param messageFormat
	 *            the detail {@code String} message format.
	 * @param messageArgs
	 *            arguments referenced by the message format specifiers in the format string. If
	 *            there are more arguments than format specifiers, the extra arguments are ignored.
	 *            The number of arguments is variable and may be zero.
	 */
	public MappingException(String messageFormat, Object... messageArgs) {
		super(String.format(messageFormat, messageArgs));
	}

	/**
	 * Constructs a new {@link RowService} exception with the specified cause and a detail message of
	 * <tt>(cause==null ? null : cause.toString())</tt> (which typically contains the class and
	 * detail message of <tt>cause</tt>). This constructor is useful for {@link RowService} exceptions
	 * that are little more than wrappers for other throwables.
	 * 
	 * @param cause
	 *            the cause (which is saved for later retrieval by the {@link #getCause()} method).
	 *            (A <tt>null</tt> value is permitted, and indicates that the cause is nonexistent
	 *            or unknown.)
	 */
	public MappingException(Throwable cause) {
		super(cause);
	}

}
