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
 * |000000|long  |int       |
 * --------------------------
 */
// TODO 将 offset 改为 int ?
public class IndexFile {
	// 一个读写锁???
	private ReentrantLock fileWriteLock = new ReentrantLock();
	
	private String path;
	private String fileName;
	private RandomAccessFile file;
	private FileChannel fileChannel;
	private MappedByteBuffer writeMappedByteBuffer;
	private ByteBuffer byteBuffer = ByteBuffer.allocate(Constants.INDEX_SIZE);

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
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constants.BUFFER_SIZE);
		} catch (IOException e) {
			throw new ClientOMSException("IndexFile create failure", e);
		}
	}

	public String appendIndex(int size) {
		String fileID;
		byte[] previousMessageFileID = new byte[6];
		long Offset;
		int previousMessageSize;
		fileWriteLock.lock();
		if (byteBuffer.remaining() == Constants.INDEX_SIZE) {
			fileID = "000000";
			Offset = 0L;
			previousMessageSize = 0;
		} else {
			byteBuffer.get(previousMessageFileID);
			Offset = byteBuffer.getLong();
			previousMessageSize = byteBuffer.getInt();
			int name = Integer.valueOf(new String(previousMessageFileID));
			Offset += previousMessageSize;
			if (Offset + size > Constants.LOG_FILE_SIZE) {
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
		fileWriteLock.unlock();
		return fileID + ":" + Offset;
	}

	public byte[] readIndexByOffset(long offset) {

		return null;
	}

	public String getFileName(){
		return this.fileName;
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
