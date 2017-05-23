package io.openmessaging.demo;

import java.util.concurrent.ConcurrentHashMap;

public class CommitLogHandler {
	private static ConcurrentHashMap<String, CommitLog> handler = new ConcurrentHashMap<>();

	private CommitLogHandler() {
	}

	public static CommitLog getCommitLogByName(String name) {
		if (handler.containsKey(name)) {
			return handler.get(name);
		}
		return handler.put(name, new CommitLog());
	}
}
