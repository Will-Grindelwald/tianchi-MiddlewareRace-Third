package io.openmessaging.demo;

import java.util.concurrent.ConcurrentHashMap;

public class CommitLogHandler2 {
	private static final ConcurrentHashMap<String, CommitLog2> handler = new ConcurrentHashMap<>();

	private CommitLogHandler2() {
	}

	public static CommitLog2 getCommitLogByName(String path, String name) {
		if (handler.containsKey(name)) {
			return handler.get(name);
		} else { // TODO ????
			handler.put(name, new CommitLog2(path + "/" + name));
		}
		return handler.get(name);
	}
}
