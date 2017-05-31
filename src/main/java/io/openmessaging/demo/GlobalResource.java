package io.openmessaging.demo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

// 全局资源, 应最大限度保证并发
public class GlobalResource {

	private static final ConcurrentHashMap<String, Topic> topicHandler = new ConcurrentHashMap<>();
	public static AtomicInteger count = new AtomicInteger(0); //// test

	private static final LinkedBlockingQueue<WriteTask> WriteTaskBlockQueue = new LinkedBlockingQueue<>(
			Constants.BLOCKING_QUEUE_SIZE);

	// 用于 Buffer ReMap 的线程池
	private static final ExecutorService BufferReMapExecPool = Executors
			.newFixedThreadPool(Constants.REMAP_THREAD_CONUT);

	// 用于 WriteMessageToLogFile 的线程池
	private static final ExecutorService WriteMessageExecPool = Executors
			.newFixedThreadPool(Constants.WRITE_MESSAGE_THREAD_CONUT);
	static {
		for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT; i++) {
			WriteMessageExecPool.submit(new WriteMessageService());
		}
	}

	private GlobalResource() {
	}

	public static Topic getTopicByName(String bucket) {
		return topicHandler.computeIfAbsent(bucket, bucketName -> {
			count.incrementAndGet(); //// test
			return new Topic(bucketName);
		});
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

	public static Future<?> submitReMapTask(Runnable task) {
		return BufferReMapExecPool.submit(task);
	}

}
