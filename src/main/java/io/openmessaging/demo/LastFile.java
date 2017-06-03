package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LastFile {
	private final RandomAccessFile lastFile;
	public volatile int nextMessageOffset;

	public LastFile(String path) {
		File last = new File(path, Constants.LAST_FILE_NAME);
		try {
			if (!last.exists()) {
				last.createNewFile();
			}
			lastFile = new RandomAccessFile(last, "rw");
			if (last.length() != Constants.LAST_FILE_SIZE) {
				nextMessageOffset = 0;
			} else {
				nextMessageOffset = lastFile.readInt();
			}
		} catch (IOException e) {
			throw new ClientOMSException("LastFile create failure", e);
		}
	}

	// 只会从 writeeBuffer 调用, 不会并发
	public void flush() {
		try {
			lastFile.seek(0);
			lastFile.writeInt(nextMessageOffset);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
