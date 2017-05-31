package io.openmessaging.demo;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// 一个 buchet 一个, 全局唯一, 小心并发
public class Topic {

	public final String bucket;
	private final String path;

	// Last file
	private final LastFile lastFile;

	// IndexFile
	private final CopyOnWriteArrayList<PersistenceFile> indexFileList = new CopyOnWriteArrayList<>();
	// private WriteBuffer writeIndexFileBuffer;
	private WriteBuffer2 writeIndexFileBuffer;

	// LogFiles
	private final CopyOnWriteArrayList<PersistenceFile> logFileList = new CopyOnWriteArrayList<>();
	// private WriteBuffer writeLogFileBuffer;
	private WriteBuffer2 writeLogFileBuffer;

	public Topic(String bucket) {
		this.bucket = bucket;
		path = System.getProperty("path") + "/" + bucket;
		// topic dir
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + " 不是一个目录");
		} else {
			file.mkdirs();
		}
		// Last file
		lastFile = new LastFile(path);
		// indexFile
		int tmpFileID;
		for (String indexFile : file.list((dir, name) -> name.startsWith(Constants.INDEX_FILE_PREFIX))) {
			try {
				tmpFileID = Integer.valueOf(indexFile.substring(Constants.INDEX_FILE_PREFIX.length()));
			} catch (NumberFormatException e) {
				System.err.println("indexFile name 错误");
				continue;
			}
			indexFileList.add(new PersistenceFile(path, tmpFileID, Constants.INDEX_FILE_PREFIX));
		}
		if (indexFileList.isEmpty()) {
			indexFileList.add(new PersistenceFile(path, 0, Constants.INDEX_FILE_PREFIX));
		}
		writeIndexFileBuffer = new WriteBuffer2(Constants.INDEX_FILE_PREFIX, indexFileList,
				lastFile.getNextIndexOffset(), 0);
		// LogFiles
		for (String indexFile : file.list((dir, name) -> name.startsWith(Constants.LOG_FILE_PREFIX))) {
			try {
				tmpFileID = Integer.valueOf(indexFile.substring(Constants.LOG_FILE_PREFIX.length()));
			} catch (NumberFormatException e) {
				System.err.println("logFile name 错误");
				continue;
			}
			logFileList.add(new PersistenceFile(path, tmpFileID, Constants.LOG_FILE_PREFIX));
		}
		if (logFileList.isEmpty()) {
			logFileList.add(new PersistenceFile(path, 0, Constants.LOG_FILE_PREFIX));
		}
		writeLogFileBuffer = new WriteBuffer2(Constants.LOG_FILE_PREFIX, logFileList, lastFile.getNextMessageOffset(),
				Constants.BUFFER_SIZE);
	}

	// for Producer
	public long appendIndex(int size) throws InterruptedException {
		return lastFile.updateAndAppendIndex(size, writeIndexFileBuffer);
	}

	// for WriteMessageService
	public void appendMessageBytes(byte[] bytes, long offset) throws InterruptedException {
		writeLogFileBuffer.write(bytes, offset);
	}

	// for Consumer
	public List<PersistenceFile> getIndexFileList() {
		return indexFileList;
	}

	// for Consumer
	public List<PersistenceFile> getLogFileList() {
		return logFileList;
	}

	public long getNextIndexOffset() {
		return lastFile.getNextIndexOffset();
	}
	// for Producer
	public void flush() throws InterruptedException {
		// 1. update lastIndex
		lastFile.flush();
		// 2. flush writeIndexFileBuffer
		writeIndexFileBuffer.flush();
		// 3. flush writeLogFileBuffer
		writeLogFileBuffer.flush();
	}

}
