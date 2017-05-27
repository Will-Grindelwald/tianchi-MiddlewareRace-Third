package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

// 全局唯一, 小心并发
public class IndexFile {
	// 一个读写锁???
	private ReentrantLock fileWriteLock = new ReentrantLock();

	private final String path;
	private final String fileName;
	private final RandomAccessFile file;
	private final FileChannel fileChannel;

	// 目前下面三个变量仅用于 appendIndex 和 flush
	private MappedByteBuffer writeMappedByteBuffer;
	private final ByteBuffer lastIndex = ByteBuffer.allocate(Constants.INDEX_SIZE);
	private final byte[] lastIndex0 = new byte[Constants.INDEX_SIZE];
	private final AtomicInteger count = new AtomicInteger(0);

	public IndexFile(String path, String fileName, int offset) {
		this.path = path;
		this.fileName = fileName;
		File file = new File(path, fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
			this.fileChannel = this.file.getChannel();
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset,
					Constants.INDEX_WRITE_BUFFER_SIZE); // 1024 * 1024 条 index, 从 offset 开始映射
		} catch (IOException e) {
			throw new ClientOMSException("IndexFile create failure", e);
		}
	}

	// for Producer
	public String appendIndex(int size) {
//		fileWriteLock.lock();

		String fileID;
		byte[] previousMessageFileID = new byte[6];
		int Offset;
		int previousMessageSize;
		if (lastIndex.remaining() == Constants.INDEX_SIZE) {
			fileID = "000000";
			Offset = 0;
			previousMessageSize = 0;
		} else {

			lastIndex.flip();
			lastIndex.get(previousMessageFileID);
			Offset = lastIndex.getInt();
			previousMessageSize = lastIndex.getInt();
			int name = Integer.valueOf(new String(previousMessageFileID));
			Offset += previousMessageSize;
			if (Offset + size > Constants.LOG_FILE_SIZE) {
				fileID = String.format("%06d", name + 1);
				Offset = 0;
			} else {
				fileID = String.format("%06d", name);
			}
			lastIndex.clear();
		}
		lastIndex.put(fileID.getBytes());
		lastIndex.putInt(Offset);
		lastIndex.putInt(size);
		lastIndex.flip();
		if (writeMappedByteBuffer.remaining() < lastIndex.limit()) {
			// writeMappedByteBuffer.flip();
			writeMappedByteBuffer.force();
			// writeMappedByteBuffer.clear();
			try {
				writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
						count.incrementAndGet() * Constants.INDEX_WRITE_BUFFER_SIZE, Constants.INDEX_WRITE_BUFFER_SIZE);
			} catch (IOException e) {
				System.out.println("MappedByteBuffer Exception");
				e.printStackTrace();
			}
		}
		writeMappedByteBuffer.put(lastIndex);
//		fileWriteLock.unlock();
		return fileID + ":" + Offset;
	}

	// for Producer
	public Index appendIndex0(int size) {
		fileWriteLock.lock(); // 获得 lastIndex0 及 writeMappedByteBuffer 的独占权
		int lastFileID = Index.getFileID(lastIndex0);
		int newOffset = Index.getOffset(lastIndex0) + Index.getSize(lastIndex0);
		if (newOffset + size > Constants.LOG_FILE_SIZE) { // 启用新的 LogFile
			lastFileID++;
			Index.setFileID(lastIndex0, lastFileID);
			newOffset = 0;
		}
		Index.setOffset(lastIndex0, newOffset);
		Index.setSize(lastIndex0, size);
		if (writeMappedByteBuffer.remaining() < lastIndex0.length) {
			writeMappedByteBuffer.force();
			try {
				writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
						count.incrementAndGet() * Constants.INDEX_WRITE_BUFFER_SIZE, Constants.INDEX_WRITE_BUFFER_SIZE);
			} catch (IOException e) {
				System.out.println("MappedByteBuffer Exception");
				e.printStackTrace();
			}
		}
		writeMappedByteBuffer.put(lastIndex0);
		Index lastIndexToreturn = new Index(lastFileID, newOffset,
				count.get() * Constants.INDEX_WRITE_BUFFER_SIZE + writeMappedByteBuffer.position());
		fileWriteLock.unlock();
		return lastIndexToreturn;
	}

	// for Consumer
	public FileChannel getFileChannel() {
		return this.fileChannel;
	}

	// for Producer
	public void flush() {
		try {
			writeMappedByteBuffer.force();
			fileChannel.force(false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
