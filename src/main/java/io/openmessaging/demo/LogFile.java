package io.openmessaging.demo;

public class LogFile {
	private long fileSize;

	public LogFile(String path, long fileSize) {
		this.fileSize = fileSize;
	}

	public void doAppend() {

	}

	
	public long getSize(){
		return fileSize;
	}
	
	private void flush() {

	}

}
