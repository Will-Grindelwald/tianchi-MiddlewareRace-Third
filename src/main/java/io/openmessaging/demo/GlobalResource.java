package io.openmessaging.demo;

import java.util.concurrent.ConcurrentHashMap;

// 全局资源, 应最大限度保证并发
public class GlobalResource {
	private static boolean close = false;

	private static int count = 0;
	private static final ConcurrentHashMap<String, Topic> topicHandler = new ConcurrentHashMap<>();

	private GlobalResource() {
	}

	public static Topic getTopicByName(String bucket) {
		return topicHandler.computeIfAbsent(bucket, bucketName -> new Topic(bucketName));
	}

}
