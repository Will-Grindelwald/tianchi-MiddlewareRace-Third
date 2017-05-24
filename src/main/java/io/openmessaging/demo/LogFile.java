package io.openmessaging.demo;

public class LogFile extends MappedFile {
	private long fileSize;

	public LogFile(String path, String fileName, long fileSize) {
		super(path, fileName);
		this.fileSize = fileSize;
	}

	public void doAppend() {

	}

	public long getSize() {
		return fileSize;
	}

	@Override
	public void flush() {

	}

}
