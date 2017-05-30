package io.openmessaging.demo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class GlobalResource {

	private static final ConcurrentHashMap<String, Topic> topicHandler = new ConcurrentHashMap<>();

	private static final LinkedBlockingQueue<WriteTask> WriteTaskBlockQueue = new LinkedBlockingQueue<>();

	// 用于 WriteMessageToLogFile 的线程池
	private static final ExecutorService WriteMessageExecPool = Executors
			.newFixedThreadPool(Constants.WRITE_MESSAGE_THREAD_CONUT);
	static {
		for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT; i++) {
			WriteMessageExecPool.submit(new WriteMessageService());
		}
	}

	// 用于 Buffer ReMap 的线程池
	public static final ExecutorService BufferReMapExecPool = Executors
			.newFixedThreadPool(Constants.REMAP_THREAD_CONUT);

	private GlobalResource() {
	}

	public static synchronized Topic getTopicByName(String bucket) {
		return topicHandler.computeIfAbsent(bucket, bucketName -> new Topic(bucket));
	}

	public static void putWriteTask(WriteTask writeTask) throws InterruptedException {
		WriteTaskBlockQueue.put(writeTask);
	}

	public static WriteTask takeWriteTask() throws InterruptedException {
		return WriteTaskBlockQueue.take();
	}

	public static int getSizeOfWriteTaskBlockQueue() throws InterruptedException {
		return WriteTaskBlockQueue.size();
	}

}
