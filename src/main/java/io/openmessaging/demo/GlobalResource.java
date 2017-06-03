package io.openmessaging.demo;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

// 全局资源, 应最大限度保证并发
public class GlobalResource {
	private static boolean close = false;

	private static int count = 0;
	private static final ConcurrentHashMap<String, Topic> topicHandler = new ConcurrentHashMap<>();

	@SuppressWarnings("unchecked")
	private static final LinkedBlockingQueue<WriteTask>[] WriteTaskBlockQueueArray = new LinkedBlockingQueue[Constants.WRITE_MESSAGE_THREAD_CONUT];

	// 用于 WriteMessageToLogFile 的线程池
	private static final ExecutorService WriteMessageExecPool = Executors
			.newFixedThreadPool(Constants.WRITE_MESSAGE_THREAD_CONUT);

	static {
		for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT; i++) {
			WriteTaskBlockQueueArray[i] = new LinkedBlockingQueue<WriteTask>();
			WriteMessageExecPool.submit(new WriteMessageService(i));
		}
	}

	// for test
//	private static final ScheduledExecutorService test = Executors.newSingleThreadScheduledExecutor();
//	static {
//		test.scheduleAtFixedRate(() -> {
//			try {
//				for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT; i++) {
//					System.out.println("Q" + i + ":" + getSizeOfWriteTaskBlockQueue(i));
//				}
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}, 100, 1000, TimeUnit.MILLISECONDS);
//	}
//
//	// for test
//	public static int getSizeOfWriteTaskBlockQueue(int ID) throws InterruptedException {
//		return WriteTaskBlockQueueArray[ID].size();
//	}

	private GlobalResource() {
	}

	public static Topic getTopicByName(String bucket) {
		return topicHandler.computeIfAbsent(bucket, bucketName -> {
			Topic topic = new Topic(bucketName, (count++) % Constants.WRITE_MESSAGE_THREAD_CONUT);
			return topic;
		});
	}

	public static void putWriteTask(int ID, WriteTask writeTask) throws InterruptedException {
		WriteTaskBlockQueueArray[ID].put(writeTask);
	}

	public static WriteTask takeWriteTask(int ID) throws InterruptedException {
		return WriteTaskBlockQueueArray[ID].take();
	}

	public static boolean WriteTaskBlockQueueIsEmpty() throws InterruptedException {
		boolean empty = true;
		for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT && empty; i++) {
			empty &= WriteTaskBlockQueueArray[i].isEmpty();
		}
		return empty;
	}

	public static synchronized void flush() {
		if (!close) {
			close = true;
			try {
				while (!GlobalResource.WriteTaskBlockQueueIsEmpty()) {
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
