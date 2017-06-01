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
	private final WriteBuffer3 writeIndexFileBuffer;

	// LogFiles
	private final CopyOnWriteArrayList<PersistenceFile> logFileList = new CopyOnWriteArrayList<>();
	private final WriteBuffer3 writeLogFileBuffer;

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
		// indexFile 及其 WriteBuffer
		int tmpFileID;
		for (String indexFile : file.list((dir, name) -> name.startsWith(Constants.INDEX_FILE_PREFIX))) {
			try {
				tmpFileID = Integer.valueOf(indexFile.substring(Constants.INDEX_FILE_PREFIX.length()));
				System.out.println(bucket + "index:" + tmpFileID);
			} catch (NumberFormatException e) {
				System.err.println("indexFile name 错误");
				continue;
			}
			indexFileList.add(new PersistenceFile(path, tmpFileID, Constants.INDEX_FILE_PREFIX));
		}
		if (indexFileList.isEmpty()) {
			indexFileList.add(new PersistenceFile(path, 0, Constants.INDEX_FILE_PREFIX));
		}
		writeIndexFileBuffer = new WriteBuffer3(Constants.INDEX_FILE_PREFIX, indexFileList,
				lastFile.getNextIndexOffset());
		// LogFiles 及其 WriteBuffer
		for (String indexFile : file.list((dir, name) -> name.startsWith(Constants.LOG_FILE_PREFIX))) {
			try {
				tmpFileID = Integer.valueOf(indexFile.substring(Constants.LOG_FILE_PREFIX.length()));
				System.out.println(bucket + "log:" + tmpFileID);
			} catch (NumberFormatException e) {
				System.err.println("logFile name 错误");
				continue;
			}
			logFileList.add(new PersistenceFile(path, tmpFileID, Constants.LOG_FILE_PREFIX));
		}
		if (logFileList.isEmpty()) {
			logFileList.add(new PersistenceFile(path, 0, Constants.LOG_FILE_PREFIX));
		}
		writeLogFileBuffer = new WriteBuffer3(Constants.LOG_FILE_PREFIX, logFileList, lastFile.getNextMessageOffset());
	}

	// for Producer, 由 send -> putMessage 单线程调用
	public long appendIndex(int size) throws InterruptedException {
		return lastFile.updateAndAppendIndex(size, writeIndexFileBuffer);
	}

	public WriteBuffer3 getWriteIndexFileBuffer() {
		return writeIndexFileBuffer;
	}

	public WriteBuffer3 getWriteLogFileBuffer() {
		return writeLogFileBuffer;
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
//		writeIndexFileBuffer.flush();
		// 3. flush writeLogFileBuffer
//		writeLogFileBuffer.flush();
	}

}
