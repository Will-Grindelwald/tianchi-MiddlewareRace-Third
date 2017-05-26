package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 索引文件结构：
 *  -------------------------- 
 *  |fileID|offset|mesagesize|
 *  |000000|long|int | 
 *  --------------------------
 */
// TODO 将 offset 改为 int ?
public class IndexFile {
	public static final int INDEX_SIZE = 18; // 6 + 8 + 4
	public static final int INDEX_FILE_SIZE = INDEX_SIZE * 1024 * 1024;
    private static AtomicInteger count = new AtomicInteger(0);

	// 一个读写锁???
	private ReentrantLock fileWriteLock = new ReentrantLock();

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
			throw new ClientOMSException("IndexFile create failure", e);
		}
	}

	public String appendIndex(int size) {
		fileWriteLock.lock();
		String fileID;
		byte[] previousMessageFileID = new byte[6];
		long Offset;
		int previousMessageSize;

		if (byteBuffer.remaining() == INDEX_SIZE) {
			fileID = "000000";
			Offset = 0L;
			previousMessageSize = 0;
		} else {
			byteBuffer.get(previousMessageFileID);
			System.out.println(new String(previousMessageFileID));
			Offset = byteBuffer.getLong();
			previousMessageSize = byteBuffer.getInt();
			int name = Integer.valueOf(new String(previousMessageFileID));
			Offset += previousMessageSize;
			if (Offset + size > CommitLog.LOG_FILE_SIZE) {
				fileID = String.format("%06d", name + 1);
				Offset = 0;
			} else {
				fileID = String.format("%06d", name);
			}
			byteBuffer.clear();
		}
		byteBuffer.put(fileID.getBytes());
		byteBuffer.putLong(Offset);
		byteBuffer.putInt(size);
		byteBuffer.flip();
		if (writeMappedByteBuffer.remaining() < byteBuffer.limit()) {
			flush();
		}
		writeMappedByteBuffer.put(byteBuffer);
		byteBuffer.flip();
		fileWriteLock.unlock();
		return fileID + ":" + Offset;
	}

	public byte[] readIndexByOffset(long offset) {

		return null;
	}

	public String getFileName() {
		return this.fileName;
	}

	public void flush() {
//		writeMappedByteBuffer.flip();
		writeMappedByteBuffer.force();
		writeMappedByteBuffer.clear();
		try {
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, count.incrementAndGet() * INDEX_FILE_SIZE, INDEX_FILE_SIZE);
		} catch (IOException e) {
			System.out.println("MappedByteBuffer Exception");
			e.printStackTrace();
		}

	}

}
