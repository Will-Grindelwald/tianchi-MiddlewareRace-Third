package io.openmessaging.demo;

public class Constants {
	// for index
	public static final int OFFSET_POS = 0;
	public static final int SIZE_POS = 8;
	public static final int INDEX_SIZE = 12;

	// for buffer
	public static final int BUFFER_SIZE = INDEX_SIZE * 2 * 1024 * 1024; // buffer 的大小限制 24 M TODO 待测 12 M?

	// for logFile and indexFile
	public static final int FILE_SIZE = BUFFER_SIZE * 50; // 文件的大小限制 50 个 Buffer 大小
	public static final String INDEX_FILE_PREFIX = "Index";
	public static final String LOG_FILE_PREFIX = "LOG";
	public static final String LAST_FILE_NAME = "Last";

	// for Mutli-Thread
	public static final int REMAP_THREAD_CONUT = 5; // 用于 Buffer ReMap 的线程池 TODO 待测
	public static final int WRITE_MESSAGE_THREAD_CONUT = 10; // 用于 Buffer ReMap 的线程池 TODO 待测

}
