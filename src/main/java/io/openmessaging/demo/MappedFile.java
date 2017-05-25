package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public abstract class MappedFile {
	private String fileName;
	private RandomAccessFile file;
	private FileChannel fileChannel;

	public MappedFile(String path, String fileName) {
		this.fileName = fileName;
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

	public FileChannel getFileChannel() {
		return this.fileChannel;
	}
	
	public String getFileName(){
		return this.fileName;
	}

	public abstract void flush();

}
