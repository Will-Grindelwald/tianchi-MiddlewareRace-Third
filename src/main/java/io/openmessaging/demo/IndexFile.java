package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile extends MappedFile {
	// 一个读写锁???
	private static ReentrantLock fileWriteLock = new ReentrantLock();
	/**
	 * 索引文件结构：
	 * ----------------------------
	 * |fileName|offset|mesagesize|
	 * |LOG000000|long |int       |
	 * ----------------------------
	 */
	private long offset;
	private int indexSize = 30;

	private static final int ONEINDEXSIZE = 30 * 1024;
	private static final String NAMEFIRST = "LOG";

	private MappedByteBuffer readMappedByteBuffer, writeMappedByteBuffer;
	private ByteBuffer byteBuffer = ByteBuffer.allocate(ONEINDEXSIZE);

	public IndexFile(String path, String fileName) {
		super(path, fileName);
	}

	public void appendIndex(int size) {
		String fileName = null;
		fileWriteLock.lock();

		if (byteBuffer.remaining() == ONEINDEXSIZE) {
			fileName = NAMEFIRST + "000000";
			offset = 0L;
		} else {

		}
		byteBuffer.put(fileName.getBytes());
		byteBuffer.putLong(offset);
		byteBuffer.putInt(size);
		fileWriteLock.unlock();
	}

	public byte[] readIndexByOffset(long offset) {

		return null;
	}

	@Override
	public void flush() {

	}

}
