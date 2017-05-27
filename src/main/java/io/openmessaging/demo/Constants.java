package io.openmessaging.demo;

public class Constants {
	// for index
	public static final int FILEID_POS = 0;
	public static final int FILEID_LEN = "000000".getBytes().length; // 6
	public static final int OFFSET_POS = FILEID_POS + FILEID_LEN;
	public static final int OFFSET_LEN = Utils.intToByteArray(0).length; // 4
	public static final int SIZE_POS = OFFSET_POS + OFFSET_LEN;
	public static final int SIZE_LEN = Utils.intToByteArray(0).length; // 4
	public static final int INDEX_SIZE = SIZE_POS + SIZE_LEN;
	public static final int INDEX_WRITE_BUFFER_SIZE = INDEX_SIZE * 1024 * 1024;

	// for log
	public static final int LOG_FILE_SIZE = 100 * 1024 * 1024; // LogFile 文件的大小限制

	// for buffer
	public static final int BUFFER_SIZE = 20 * 1024 * 1024;

	// for write buffer
	public static final int BYTE_SIZE = 3 * BUFFER_SIZE;
}
