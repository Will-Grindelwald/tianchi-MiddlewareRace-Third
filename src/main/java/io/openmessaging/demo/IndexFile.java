package io.openmessaging.demo;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
	 * |LOG000000|long |int       |
	 * ----------------------------
	 */
	private String fileName; 
	private long offset;
	private int messageSize;
	
	private static final int ONEINDEXSIZE=30*1024;
	private static final String NAMEFIRST="LOG";
	
	private MappedByteBuffer mappedByteBuffer;
	private ByteBuffer byteBuffer=ByteBuffer.allocate(ONEINDEXSIZE);

	private RandomAccessFile indexFile;
	private FileChannel fileChannel;

	public IndexFile(RandomAccessFile indexFile) {
		this.indexFile = indexFile;
		this.fileChannel = indexFile.getChannel();
	}


	public void appendIndex(int size){
			fileWriteLock.lock();
			
			
			if(byteBuffer.remaining()==ONEINDEXSIZE){
				fileName=NAMEFIRST+"000000";
				offset=0L;
			}
			else{
				
			}
			byteBuffer.put(fileName.getBytes());
			byteBuffer.putLong(offset);
			byteBuffer.putInt(size);
			fileWriteLock.unlock();
	}

	public byte[] readIndexByOffset(long offset) {
		return null;
	}

	public void flush() {

	}

}
