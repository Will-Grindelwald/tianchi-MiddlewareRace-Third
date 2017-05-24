package io.openmessaging.demo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile extends MappedFile {
	// 一个读写锁???
	private static ReentrantLock fileWriteLock = new ReentrantLock();
	/**
	 * 索引文件结构： 
	 * ---------------------------- 
	 * |fileName|offset|mesagesize|
	 * |000000|long |int | 
	 * ----------------------------
	 */
	private long offset;
	private static final int ONEINDEXSIZE = 18;
//	private static AtomicInteger count = new AtomicInteger(0);
	private static final int INDEXSIZE = ONEINDEXSIZE * 1024*1024;
	// private static final String NAMEFIRST = "LOG";

	private MappedByteBuffer readMappedByteBuffer, writeMappedByteBuffer;
	private ByteBuffer byteBuffer = ByteBuffer.allocate(ONEINDEXSIZE);

	private FileChannel fileChannel;

	public IndexFile(String path, String fileName) {
		super(path, fileName);
		init();
	}

	public void init() {
		fileChannel = super.getFileChannel();
		try {
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEXSIZE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String appendIndex(int size) {
		fileWriteLock.lock();
		String fileName = null;
		if (byteBuffer.remaining() == ONEINDEXSIZE) {
			fileName = "000000";
			offset = 0L;

		} else {
			byte[] tmpname = new byte[6];
			byteBuffer.get(tmpname);
			offset = byteBuffer.getLong();
			int lastSize = byteBuffer.getInt();
			offset += lastSize;
			int name = Integer.valueOf(new String(tmpname));
			if (offset + size > CommitLog.LOG_FILE_SIZE) {
				fileName = String.format("%06d", name + 1);
				offset = 0;
			} else {
				fileName = String.format("%06d", name);
			}

		}
		byteBuffer.put(fileName.getBytes());
		byteBuffer.putLong(offset);
		byteBuffer.putInt(size);
		byteBuffer.flip();
		if (writeMappedByteBuffer.remaining() >= byteBuffer.limit()) {
			writeMappedByteBuffer.put(byteBuffer);
		} else {
			flush();
		}
		fileWriteLock.unlock();
		return fileName+":"+offset;
	}

	public byte[] readIndexByOffset(long offset) {

		return null;
	}

	@Override
	public void flush() {
		writeMappedByteBuffer.flip();
		writeMappedByteBuffer.force();
		writeMappedByteBuffer.clear();
//		try {
//			int i = count.incrementAndGet();
//			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, i * INDEXSIZE, 2 * i * INDEXSIZE);
//		} catch (IOException e) {
//			System.out.println("MappedByteBuffer Exception");
//			e.printStackTrace();
//		}

	}

}
