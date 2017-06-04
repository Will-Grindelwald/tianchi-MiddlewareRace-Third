package io.openmessaging.demo;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

// 全局资源, 应最大限度保证并发
public class GlobalResource {
	private static final ConcurrentHashMap<String, Topic> topicHandler = new ConcurrentHashMap<>();
	public static final AtomicInteger count = new AtomicInteger(0);

	private static final LinkedBlockingQueue<WriteTask> WriteTaskBlockQueue = new LinkedBlockingQueue<>();

	private static final ExecutorService WriteMessageExecPool = Executors
			.newFixedThreadPool(Constants.WRITE_MESSAGE_THREAD_CONUT);
	static {
		for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT; i++) {
			WriteMessageExecPool.submit(new WriteMessageService());
		}
	}

	private GlobalResource() {
	}

//	// for test
//	private static final ScheduledExecutorService test = Executors.newSingleThreadScheduledExecutor();
//	static {
//		test.scheduleAtFixedRate(() -> System.out.println("Q:" + WriteTaskBlockQueue.size()), 100, 100,
//				TimeUnit.MILLISECONDS);
//	}
	
	public static Topic getTopicByName(String bucket) {
		return topicHandler.computeIfAbsent(bucket, bucketName -> new Topic(bucketName));
	}

	public static void putWriteTask(WriteTask writeTask) throws InterruptedException {
		WriteTaskBlockQueue.put(writeTask);
	}

	public static WriteTask takeWriteTask() throws InterruptedException {
		return WriteTaskBlockQueue.take();
	}

	public static void flush() {
		if (count.decrementAndGet() == 0) {
			try {
				Iterator<Map.Entry<String, Topic>> iterator = topicHandler.entrySet().iterator();
				while (iterator.hasNext()) {
					iterator.next().getValue().flush();
				}
				do {
					// 全局的 WriteTaskQueue 非空
					Thread.sleep(50);
				} while (!WriteTaskBlockQueue.isEmpty());
				WriteMessageExecPool.shutdown();
				do {
					// 全局的 WriteTaskQueue 非空
					Thread.sleep(1);
				} while (!WriteMessageExecPool.isShutdown());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
