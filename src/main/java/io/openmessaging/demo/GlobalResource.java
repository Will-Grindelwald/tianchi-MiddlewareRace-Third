package io.openmessaging.demo;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

// 全局资源, 应最大限度保证并发
public class GlobalResource {
	private static volatile boolean close = false;

	private static final ConcurrentHashMap<String, Topic> topicHandler = new ConcurrentHashMap<>();

	private static final LinkedBlockingQueue<WriteTask> WriteTaskBlockQueue = new LinkedBlockingQueue<>(
			Constants.BLOCKING_QUEUE_SIZE);

	// private static final ArrayBlockingQueue<WriteTask> WriteTaskBlockQueue =
	// new ArrayBlockingQueue<>(
	// Constants.BLOCKING_QUEUE_SIZE);

	private static final MyQueue<Topic> ReadyTopicBlockQueue = new MyQueue<>();

	// 用于 WriteMessageToLogFile 的线程池
	private static final ExecutorService WriteMessageExecPool = Executors
			.newFixedThreadPool(Constants.WRITE_MESSAGE_THREAD_CONUT);
	static {
		for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT; i++) {
			WriteMessageExecPool.submit(new WriteMessageService());
		}
	}

	private static final ExecutorService UpdateLastTaskExecPool = Executors.newFixedThreadPool(10);
	static {
		for (int i = 0; i < 10; i++) {
			UpdateLastTaskExecPool.submit(new UpdateLastService());
		}
	}

	public static Topic takeReadyTopic() throws InterruptedException {
		return ReadyTopicBlockQueue.take();
	}

	public static void addReadyTopic(Topic topic) throws InterruptedException {
		ReadyTopicBlockQueue.add(topic);
	}

	// public static Future<?> summitUpdateTask(Runnable task) {
	// return UpdateLastTaskExecPool.submit(task);
	// }

	// for test
	// private static final ScheduledExecutorService test =
	// Executors.newSingleThreadScheduledExecutor();
	// static {
	// test.scheduleAtFixedRate(() -> System.out.println("Q" +
	// WriteTaskBlockQueue.size()), 100, 100, TimeUnit.MILLISECONDS);
	// }

	private GlobalResource() {
	}

	public static Topic getTopicByName(String bucket) {
		return topicHandler.computeIfAbsent(bucket, bucketName -> new Topic(bucketName));
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

	public static synchronized void flush() {
		if (!close) {
			close = true;
			try {
				while (GlobalResource.getSizeOfWriteTaskBlockQueue() != 0) {
					// 全局的 WriteTaskQueue 非空
					Thread.sleep(1000);
				}
				Iterator<Map.Entry<String, Topic>> iterator = topicHandler.entrySet().iterator();
				while (iterator.hasNext()) {
					iterator.next().getValue().flush();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
