package io.openmessaging.demo;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.Message;

public class CommitLog {

	public static final long LOG_FILE_SIZE = 100 * 1024 * 1024;
	private static final int BUFFER_SIZE = 20 * 1024 * 1024;
	private static final int BYTE_SIZE = 3 * BUFFER_SIZE;
	private static final int HALFBYTESIZE = BYTE_SIZE / 2;

	private static AtomicInteger shouldAppend = new AtomicInteger(0);
	private static ConcurrentHashMap<String, AtomicInteger> countFlag = new ConcurrentHashMap<>();

	private byte[] cirleBytes = new byte[BYTE_SIZE];

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
			logFileList.add(new LogFile(path, logFileName));
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

		// appendIndex是否返回Name待定
		String afterIndex = indexFile.appendIndex(size);
		String[] split = afterIndex.split(":");
		String logName = split[0];
		int offset = Integer.valueOf(split[1]);
		for (int i = offset; i <= offset + size; i++) {
			int index = i % BYTE_SIZE;
			// 提交第一部分
			if (index == 0 && i != offset) {
				// int tmp=shouldAppend.get();
				// while(countFlag.get(tmp).get()==HALFBYTESIZE){
				//// shouldAppend.set(shouldAppend.get()==0?1:0);
				//// countFlag.co
				// }
			}
			// 提交第二部分
			if (index == HALFBYTESIZE && (i / BYTE_SIZE) > 0) {
				while (countFlag.get(1).get() == HALFBYTESIZE) {
					countFlag.get(1).set(0);
				}
			} else if (index > 0 && index < BYTE_SIZE / 2) {
				countFlag.get(0).incrementAndGet();
			} else if (index >= BYTE_SIZE / 2) {
				countFlag.get(1).incrementAndGet();
			}
			cirleBytes[index] = messages[i - offset];
		}

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
