package io.openmessaging.demo;

public class Constants {
	// for buffer
	public static final int BUFFER_SIZE = 20 * 1024 * 1024;

	// for log
	public static final int LOG_FILE_SIZE = 100 * 1024 * 1024;

	// for index
	public static final int INDEX_SIZE = 14; // 6 + 4 + 4
	public static final int FILEID_POS = 0;
	public static final int OFFSET_POS = 6;
	public static final int SIZE_POS = 10;
	public static final int INDEX_WRITE_BUFFER_SIZE = INDEX_SIZE * 1024 * 1024;

	// for write buffer
	public static final int BYTE_SIZE = 3 * BUFFER_SIZE;
}
