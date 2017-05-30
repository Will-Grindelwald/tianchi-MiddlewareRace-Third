package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class GlobalResource {

	private static final Map<String, Topic> topicHandler = new HashMap<>();

	public static final LinkedBlockingQueue<WriteTask> WriteTaskBlockQueue = new LinkedBlockingQueue<>();

	// 用于 Buffer ReMap 的线程池
	public static final ExecutorService BufferReMapExecPool = Executors
			.newFixedThreadPool(Constants.REMAP_THREAD_CONUT);
	public static final ExecutorService WriteMessageExecPool = Executors
			.newFixedThreadPool(Constants.WRITE_MESSAGE_THREAD_CONUT);

	static {
		for (int i = 0; i < Constants.WRITE_MESSAGE_THREAD_CONUT; i++) {
			WriteMessageExecPool.submit(new WriteMessageService(i));
		}
	}

	private GlobalResource() {
	}

	public static synchronized Topic getTopicByName(String bucket) {
		if (topicHandler.containsKey(bucket)) {
			return topicHandler.get(bucket);
		} else { // ??
			topicHandler.put(bucket, new Topic(bucket));
		}
		return topicHandler.get(bucket);
	}
}
