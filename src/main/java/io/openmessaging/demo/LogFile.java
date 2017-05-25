package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class LogFile {
	private long fileSize;
	private String path;
	private String fileName;
	private RandomAccessFile file;
	private FileChannel fileChannel;

	public LogFile(String path, String fileName, long fileSize) {
		this.path = path;
		this.fileName = fileName;
		this.fileSize = fileSize;
		File file = new File(path, fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
		} catch (IOException e) {
			throw new ClientOMSException("indexFile create failure", e);
		}
		this.fileChannel = this.file.getChannel();
	}

	public void doAppend(byte[] bytes) {

	}

	public long getSize() {
		return fileSize;
	}

	public void flush() {

	}

}
