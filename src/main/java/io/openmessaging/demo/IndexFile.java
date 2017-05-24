package io.openmessaging.demo;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile {
	// 一个读写锁???
	private static ReentrantLock fileWriteLock = new ReentrantLock();
	/*
	 * 索引文件结构：
	 * ----------------------------
	 * |fileName|offset|mesagesize|
	 * ----------------------------
	 */
	private String fileName; 
	private Long offset;
	private Long messageSize;
	
	private MappedByteBuffer mappedByteBuffer;
	

	private RandomAccessFile indexFile;
	private FileChannel fileChannel;

	public IndexFile(RandomAccessFile indexFile) {
		this.indexFile = indexFile;
		this.fileChannel = indexFile.getChannel();
	}


	public void appendIndex(int size){
			fileWriteLock.lock();
//			if
			
			
			fileWriteLock.unlock();
	}

	public byte[] readIndexByOffset(long offset) {
		return null;
	}

	public void flush() {

	}

}
