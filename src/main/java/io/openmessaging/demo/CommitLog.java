package io.openmessaging.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.CopyOnWriteArrayList;

import io.openmessaging.Message;

public class CommitLog {

	private static final long LOG_FILE_SIZE = 100 * 1024 * 1024;


	private String path;
	private IndexFile indexFile = null;
	private CopyOnWriteArrayList<LogFile> logFileList = new CopyOnWriteArrayList<>();

	public CommitLog(String path) {
		this.path = path;
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + " 不是一个目录");
		} else {
			file.mkdirs();
		}
		File indexFile = new File(path, "indexFile");
		if (!indexFile.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				throw new ClientOMSException("indexFile create failure 1", e);
			}
		}
		try {
			this.indexFile = new IndexFile(new RandomAccessFile(indexFile, "rw"));
		} catch (FileNotFoundException e) {
			throw new ClientOMSException("indexFile create failure 2", e);
		}
		for (String logFileName : file.list((dir, name) -> name.startsWith("LOG"))) {
			logFileList.add(new LogFile(path + logFileName, LOG_FILE_SIZE));
		}

	}

	public LogFile getLastLogFile() {
		LogFile logFileLast = null;
		// 可能会有异常
		if (!logFileList.isEmpty()) {
			logFileLast = logFileList.get(logFileList.size() - 1);

		}
		return logFileLast;
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

	public void wirteIndexFile(long size) {
		LogFile lastLogFile=getLastLogFile();
//		TODO
//		indexFile.
//		if (lastLogFile== null || lastLogFile.getSize()<) {
//			getNewLogFile();
//		}
//		indexFile.writeIndexFile();
	}
}
