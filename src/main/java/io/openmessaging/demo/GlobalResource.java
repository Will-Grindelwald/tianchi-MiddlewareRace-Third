package io.openmessaging.demo;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// 全局资源, 应最大限度保证并发
public class GlobalResource {
	private static final ConcurrentHashMap<String, Topic> topicHandler = new ConcurrentHashMap<>();

	public static AtomicInteger close = new AtomicInteger(0);

	private GlobalResource() {
	}

	public static Topic getTopicByName(String bucket) {
		return topicHandler.computeIfAbsent(bucket, bucketName -> new Topic(bucketName));
	}

	public static void flush() {
		if (close.decrementAndGet() == 0) {
			Iterator<Map.Entry<String, Topic>> iterator = topicHandler.entrySet().iterator();
			while (iterator.hasNext()) {
				iterator.next().getValue().flush();
			}
		}
	}
}
