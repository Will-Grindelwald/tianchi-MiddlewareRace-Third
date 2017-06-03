package io.openmessaging.demo;

public class Constants {

	// for logFile
	public static final String LOG_FILE_NAME = "LOG";
	public static final int LOG_BUFFER_SIZE = 20 * 1024 * 1024; // 20 M

	// for Last
	public static final String LAST_FILE_NAME = "Last";
	public static final int LAST_FILE_SIZE = 4;

	// for Mutli-Thread
	public static final int WRITE_MESSAGE_THREAD_CONUT = 5;

	// for BlockingQueue
	public static final int BLOCKING_QUEUE_SIZE = 5000000;
}
