package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 本类方法皆为 synchronized, 是为互斥修改属性, 从 topic 中的调用看应该只会被单线程调用
 */
public class LastFile {
	private final String path;
	private final String fileName = Constants.LAST_FILE_NAME;
	private final RandomAccessFile lastFile;

	// 下面三个属性`互斥访问`以保证正确性
	private long nextIndexOffset;
	private long nextMessageOffset;
	private final byte[] lastIndexByte = new byte[Constants.INDEX_SIZE];

	private boolean close = false;

	public LastFile(String path) {
		this.path = path;
		File last = new File(path, fileName);
		try {
			if (!last.exists()) {
				last.createNewFile();
				lastFile = new RandomAccessFile(last, "rw");
				nextIndexOffset = 0;
				nextMessageOffset = 0;
			} else {
				lastFile = new RandomAccessFile(last, "rw");
				if (last.length() != Constants.LAST_FILE_SIZE) {
					nextIndexOffset = 0;
					nextMessageOffset = 0;
				} else {
					nextIndexOffset = lastFile.readLong();
					nextMessageOffset = lastFile.readLong();
					long offset = lastFile.readLong();
					int size = lastFile.readInt();
					Index.setOffset(lastIndexByte, offset);
					Index.setSize(lastIndexByte, size);
				}
			}
		} catch (IOException e) {
			throw new ClientOMSException("LastFile create failure", e);
		}
	}

	// 仅用于 topic 构造synchronized 
	public long getNextIndexOffset() {
		return nextIndexOffset;
	}

	// 仅用于 topic 构造 synchronized
	public long getNextMessageOffset() {
		return nextMessageOffset;
	}

	// 本身只会经由 producer.send() 被单线程调用synchronized 
	public long updateAndAppendIndex(int size, WriteBuffer2 writeIndexFileBuffer)
			throws InterruptedException {
		long newOffset = nextMessageOffset;
		nextIndexOffset += Constants.INDEX_SIZE;
		nextMessageOffset += size;
		Index.setOffset(lastIndexByte, newOffset);
		Index.setSize(lastIndexByte, size);
		writeIndexFileBuffer.write(lastIndexByte);
		if (close) {
			flush();
		}
		return newOffset;
	}
//synchronized
	public  void flush() {
		if (!close)
			close = true;
		try {
			lastFile.seek(0);
			lastFile.writeLong(nextIndexOffset);
			lastFile.writeLong(nextMessageOffset);
			lastFile.writeLong(Index.getOffset(lastIndexByte));
			lastFile.writeInt(Index.getSize(lastIndexByte));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
