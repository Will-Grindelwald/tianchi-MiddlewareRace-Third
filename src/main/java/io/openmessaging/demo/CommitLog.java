package io.openmessaging.demo;

import java.util.concurrent.CopyOnWriteArrayList;

public class CommitLog {

	private IndexFile indexFile;

	private long logFileSize;

	private CopyOnWriteArrayList<LogFile> logFileList;

	public IndexFile getLastLogFile() {
		IndexFile indexFile=null;
		while(this.indexFile!=null){
			
		}
		return indexFile;
	}
	
	public void getNewLogFile() {

	}

	public void appendMessage() {

	}

	public void fulsh() {

	}

	public void getLogFileByOffset() {

	}

	public void getMessage() {

	}

	public boolean hasNewMessage(long offset) {
		
		return false;
	}
	
	public IndexFile getIndexFile(){
		return indexFile;
	}
}
