package io.openmessaging.demo;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile {
	// 一个读写锁???
	private static ReentrantLock fileWriteLock = new ReentrantLock();

	private RandomAccessFile indexFile;
	private FileChannel fileChannel;
	private ByteBuffer bf;// or Mapped

	public IndexFile(RandomAccessFile indexFile) {
		this.indexFile = indexFile;
		this.fileChannel = indexFile.getChannel();
	}

	public void writeIndexFile() {
		fileWriteLock.lock();
	}

	public byte[] readIndexByOffset(long offset) {
		return null;
	}

	public void flush() {

	}

}
