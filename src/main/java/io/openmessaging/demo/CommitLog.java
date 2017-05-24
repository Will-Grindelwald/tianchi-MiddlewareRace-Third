package io.openmessaging.demo;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.CopyOnWriteArrayList;

import io.openmessaging.Message;

public class CommitLog {

	public static final long LOG_FILE_SIZE = 100 * 1024 * 1024;

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
		this.indexFile = new IndexFile(path, "indexFile");
		for (String logFileName : file.list((dir, name) -> name.startsWith("LOG"))) {
			logFileList.add(new LogFile(path, logFileName, LOG_FILE_SIZE));
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

	public void appendMessage(byte[] messages) {
		int size = messages.length;
		String afterIndex = indexFile.appendIndex(size);
		String[] split = afterIndex.split(":");
		String logName = split[0];
		long offset = Long.valueOf(split[1]);
		// if(logFileList.contains(o))

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

	public void putMessage(int size) {
		LogFile lastLogFile = getLastLogFile();

		indexFile.appendIndex(size);
	}

	public FileChannel getIndexFileChannel() {
		// TODO
		return null;
	}
}
