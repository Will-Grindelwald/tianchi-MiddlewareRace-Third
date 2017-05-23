package io.openmessaging.demo;

import java.io.File;
import java.util.concurrent.CopyOnWriteArrayList;

import io.openmessaging.Message;

public class CommitLog {

	private String path;
	private static final long LOG_FILE_SIZE = 100 * 1024 * 1024;

	private IndexFile indexFile;
	private CopyOnWriteArrayList<LogFile> logFileList = new CopyOnWriteArrayList<>();

	public CommitLog(String path) {
		this.path = path;
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory()) {
				throw new ClientOMSException(path + " 不是一个目录");
			}
		} else {
			file.mkdirs();
		}
		indexFile = new IndexFile(path);
		// logFileList = new LogFile(path, LOG_FILE_SIZE);
	}

	public void getLastLogFile() {

	}

	public void getNewLogFile() {

	}

	public void appendMessage() {

	}

	public void flush() {

	}

	public void getLogFileByOffset() {

	}

	public void getMessage() {

	}

	public Message getNewMessage(long offset) {
		byte[] index = indexFile.readIndexByOffset(offset);
		
		return null;
	}
}
