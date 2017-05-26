package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class LogFile {
	private String path;
	private String fileName;
	private RandomAccessFile file;
	private FileChannel fileChannel;

	public LogFile(String path, String fileName) {
		this.path = path;
		this.fileName = fileName;
		File file = new File(path, fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
			this.fileChannel = this.file.getChannel();
		} catch (IOException e) {
			throw new ClientOMSException("LogFile create failure", e);
		}
	}

	public void doAppend(byte[] bytes,int start,int end) {

	}

	public String getFileName(){
		return this.fileName;
	}

	public void flush() {

	}

}
