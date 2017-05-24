package io.openmessaging.demo;

public class LogFile {
	private long fileSize;
	private String name;

	public LogFile(String path, long fileSize) {
		this.fileSize = fileSize;
		this.name=name;
	}

	private void doAppend() {

	}
	
	public void setName(String name){
		this.name=name;
	}
	public String getName(){
		return name;
	}
	
	public long getSize(){
		return fileSize;
	}
	
	private void flush() {

	}

}
