package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LastFile {
	private final String path;
	private final String fileName = Constants.LAST_FILE_NAME;
	private final RandomAccessFile lastFile;

	public long nextIndexOffset;
	public long nextMessageOffset;
	public final byte[] lastIndexByte = new byte[Constants.INDEX_SIZE];

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
			throw new ClientOMSException("Last create failure", e);
		}
	}

	public synchronized void flush() {
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
