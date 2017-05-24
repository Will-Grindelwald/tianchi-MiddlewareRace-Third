package io.openmessaging.demo;

import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile extends MappedFile {
	// 一个读写锁???
	private static ReentrantLock fileWriteLock = new ReentrantLock();
	/*
	 * 索引文件结构：
	 * ----------------------------
	 * |fileName|offset|mesagesize|
	 * ----------------------------
	 */
	private Long offset;
	private int indexSize = 30;
	
//	private MappedByteBuffer mappedByteBuffer;
	private MappedByteBuffer readMappedByteBuffer, writeMappedByteBuffer;

	public IndexFile(String path, String fileName) {
		super(path, fileName);
	}


	public void appendIndex(int size){
			fileWriteLock.lock();
//			if
			
			
			fileWriteLock.unlock();
	}

	public byte[] readIndexByOffset(long offset) {
		
		return null;
	}

	@Override
	public void flush() {

	}

}
