package io.openmessaging.demo;

public class Constants {
	// for index
	public static final int OFFSET_POS = 0;
	public static final int SIZE_POS = 8;
	public static final int INDEX_SIZE = 12;

	// for buffer
	public static final int BUFFER_SIZE = INDEX_SIZE * 1024 * 1024; // buffer 的大小限制 12 M

	// for logFile and indexFile
	public static final int BLOCK_NUMBER = 50;
	public static final int FILE_SIZE = BUFFER_SIZE * BLOCK_NUMBER; // 文件的大小限制 50 个 Buffer 大小
	public static final String INDEX_FILE_PREFIX = "Index";
	public static final String LOG_FILE_PREFIX = "LOG";
	public static final String LAST_FILE_NAME = "Last";
	public static final int LAST_FILE_SIZE = 28;

	// for Mutli-Thread
	public static final int REMAP_THREAD_CONUT = 5; // 用于 Buffer ReMap 的线程池 TODO 待测
	public static final int WRITE_MESSAGE_THREAD_CONUT = 10; // 用于 Buffer ReMap 的线程池 TODO 待测

	public static final int BLOCKING_QUEUE_SIZE = 10000;
}
