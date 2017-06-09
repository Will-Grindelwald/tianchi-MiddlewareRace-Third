package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

// 全局唯一, 小心并发
public class IndexFile {
	private ReentrantLock fileWriteLock = new ReentrantLock();

	private final String path;
	private final String fileName;
	private final RandomAccessFile file;
	private final FileChannel fileChannel;

	// 目前下面三个变量仅用于 appendIndex 和 flush
	private MappedByteBuffer writeMappedByteBuffer;
	private final byte[] lastIndex = new byte[Constants.INDEX_SIZE];
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
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE
					, offset // 从 offset 开始映射
					, Constants.INDEX_WRITE_BUFFER_SIZE); // 1024 * 1024 条 index
		} catch (IOException e) {
			throw new ClientOMSException("IndexFile create failure", e);
		}
	}

	// for Producer
	public Index appendIndex(int size) {
		fileWriteLock.lock(); // 获得 lastIndex 及 writeMappedByteBuffer 的独占权
		int lastFileID = Index.getFileID(lastIndex);
		int newOffset = Index.getOffset(lastIndex) + Index.getSize(lastIndex);
		if (newOffset + size > Constants.LOG_FILE_SIZE) { // 启用新的 LogFile
			lastFileID++;
			Index.setFileID(lastIndex, lastFileID);
			newOffset = 0;
		}
		Index.setOffset(lastIndex, newOffset);
		Index.setSize(lastIndex, size);
		if (writeMappedByteBuffer.remaining() < lastIndex.length) {
			writeMappedByteBuffer.force();
			try {
				writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
						count.incrementAndGet() * Constants.INDEX_WRITE_BUFFER_SIZE, Constants.INDEX_WRITE_BUFFER_SIZE);
			} catch (IOException e) {
				System.out.println("MappedByteBuffer Exception");
				e.printStackTrace();
			}
		}
		writeMappedByteBuffer.put(lastIndex);
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
