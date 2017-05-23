package io.openmessaging.demo;

import java.util.concurrent.locks.ReentrantLock;

public class IndexFile {
	// 一个读写锁
	private static ReentrantLock fileWriteLock = new ReentrantLock();

	public byte[] readIndexByOffset(long offset) {
		return null;
	}
	
	public void flush() {

	}
}
