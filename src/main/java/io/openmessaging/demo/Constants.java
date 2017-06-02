package io.openmessaging.demo;

public class Constants {
	// for index
	public static final int OFFSET_POS = 0;
	public static final int SIZE_POS = 8;
	public static final int INDEX_SIZE = 12;

	// for indexFile
	public static final int INDEX_TYPE = 0;
	public static final String INDEX_FILE_PREFIX = "Index";
	// 392 * 1024 条 Index, 针对测试优化
	public static final int INDEX_BUFFER_SIZE = INDEX_SIZE * 392 * 1024;
	public static final int INDEX_FILE_BLOCK_NUMBER = 1;
	public static final int INDEX_FILE_SIZE = INDEX_BUFFER_SIZE * INDEX_FILE_BLOCK_NUMBER;

	// for logFile
	public static final int LOG_TYPE = 1;
	public static final String LOG_FILE_PREFIX = "LOG";
	public static final int LOG_BUFFER_SIZE = 20 * 1024 * 1024; // 20 M
	public static final int LOG_FILE_BLOCK_NUMBER = 3;
	public static final int LOG_FILE_SIZE = LOG_BUFFER_SIZE * LOG_FILE_BLOCK_NUMBER;

	// for Last
	public static final String LAST_FILE_NAME = "Last";
	public static final int LAST_FILE_SIZE = 28;

	// for Mutli-Thread
	// 用于 Buffer ReMap 的线程池 TODO 待测
//	public static final int REMAP_THREAD_CONUT = 10;
	// 用于 Buffer ReMap 的线程池 TODO 待测
	public static final int WRITE_MESSAGE_THREAD_CONUT = 10;

	// for BlockingQueue
	public static final int BLOCKING_QUEUE_SIZE = 8000000;
}
