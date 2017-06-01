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

	// 仅用于 topic 构造 及 ReadBuffer 判断最后一条 Index
	public long getNextIndexOffset() {
		return nextIndexOffset;
	}

	// 仅用于 topic 构造
	public long getNextMessageOffset() {
		return nextMessageOffset;
	}

	// for Producer, 由 send -> putMessage -> appendIndex 单线程调用
	public synchronized long updateAndAppendIndex(int size, WriteBuffer3 writeIndexFileBuffer)
			throws InterruptedException {
		long newOffset = nextMessageOffset;
		Index.setOffset(lastIndexByte, newOffset);
		Index.setSize(lastIndexByte, size);
		GlobalResource.putWriteTask(new WriteTask(lastIndexByte.clone(), writeIndexFileBuffer, nextIndexOffset));
		// writeIndexFileBuffer.write(lastIndexByte);
		nextIndexOffset += Constants.INDEX_SIZE;
		nextMessageOffset += size;
		if (close) {
			flush();
		}
		return newOffset;
	}

	public synchronized void flush() {
		if (!close)
			close = true;
		try {
			lastFile.seek(0);
			lastFile.writeLong(nextIndexOffset);
			lastFile.writeLong(nextMessageOffset);
			lastFile.writeLong(Index.getOffset(lastIndexByte));
			lastFile.writeInt(Index.getSize(lastIndexByte));
			System.out.println("nextIndexOffset=" + nextIndexOffset); //// test
			System.out.println("nextMessageOffset=" + nextMessageOffset); //// test
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
