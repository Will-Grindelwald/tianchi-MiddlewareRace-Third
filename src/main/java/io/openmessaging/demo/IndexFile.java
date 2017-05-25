package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 索引文件结构：
 * --------------------------
 * |fileID|offset|mesagesize|
 * |000000|int   |int       |
 * --------------------------
 */
// TODO 将 offset 改为 int
public class IndexFile {
	// 一个读写锁???
	private ReentrantLock fileWriteLock = new ReentrantLock();

	private long offset;
	public static final int INDEX_SIZE = 14;
	// private static AtomicInteger count = new AtomicInteger(0);
	private static final int INDEX_FILE_SIZE = INDEX_SIZE * 1024 * 1024;
	// private static final String NAMEFIRST = "LOG";
	private String path;
	private String fileName;
	private RandomAccessFile file;
	private FileChannel fileChannel;
	private MappedByteBuffer writeMappedByteBuffer;
	private ByteBuffer byteBuffer = ByteBuffer.allocate(INDEX_SIZE);

	public IndexFile(String path, String fileName) {
		this.path = path;
		this.fileName = fileName;
		File file = new File(path, fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
			this.fileChannel = this.file.getChannel();
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_FILE_SIZE);
		} catch (IOException e) {
			throw new ClientOMSException("indexFile create failure", e);
		}
	}

	public String appendIndex(int size) {
		fileWriteLock.lock();
		String fileName = null;
		if (byteBuffer.remaining() == INDEX_SIZE) {
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
		return fileName + ":" + offset;
	}

	public byte[] readIndexByOffset(long offset) {

		return null;
	}

	public void flush() {
		writeMappedByteBuffer.flip();
		writeMappedByteBuffer.force();
		writeMappedByteBuffer.clear();
		// try {
		// int i = count.incrementAndGet();
		// writeMappedByteBuffer =
		// fileChannel.map(FileChannel.MapMode.READ_WRITE, i * INDEXSIZE, 2 * i
		// * INDEXSIZE);
		// } catch (IOException e) {
		// System.out.println("MappedByteBuffer Exception");
		// e.printStackTrace();
		// }

	}

}
