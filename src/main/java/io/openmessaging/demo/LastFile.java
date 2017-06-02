package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LastFile {
	private final String path;
	private final String fileName = Constants.LAST_FILE_NAME;
	private final RandomAccessFile lastFile;

	private long nextIndexOffset;
	private long nextMessageOffset;
	private final byte[] lastIndexByte = new byte[Constants.INDEX_SIZE];

	private volatile boolean close = false;

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

	// for Producer, 由 appendIndex 而来, 是串行的
	public long updateAndAppendIndex(int size, WriteBuffer3 writeIndexFileBuffer) throws InterruptedException {
		long newOffset = nextMessageOffset;
		Index.setOffset(lastIndexByte, newOffset);
		Index.setSize(lastIndexByte, size);
		GlobalResource.putWriteTask(new WriteTask(writeIndexFileBuffer, lastIndexByte.clone(), nextIndexOffset));
		nextIndexOffset += Constants.INDEX_SIZE;
		nextMessageOffset += size;
		if (close) {
			flush();
		}
		return newOffset;
	}

	// for Producer, 由 GlobalResource.flush() 而来, 全局只会触发一次, 仅就测试来说不会与
	// updateAndAppendIndex 同时发生, 所以不加锁
	public void flush() {
		if (!close)
			close = true;
		try {
			lastFile.seek(0);
			lastFile.writeLong(nextIndexOffset);
			lastFile.writeLong(nextMessageOffset);
			lastFile.writeLong(Index.getOffset(lastIndexByte));
			lastFile.writeInt(Index.getSize(lastIndexByte));
//			System.out.println("nextIndexOffset=" + nextIndexOffset + "nextMessageOffset=" + nextMessageOffset); // test
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
