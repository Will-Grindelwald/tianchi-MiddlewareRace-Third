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
				lastFile.getNextIndexOffset(), Constants.INDEX_TYPE);
		// LogFiles 及其 WriteBuffer
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
		writeLogFileBuffer = new WriteBuffer3(Constants.LOG_FILE_PREFIX, logFileList, lastFile.getNextMessageOffset(), Constants.LOG_TYPE);
	}

	// for Producer, 串行
	public synchronized void putMessage(byte[] messageByte) throws InterruptedException {
		// 2. 添加 Index
		long offset = lastFile.updateAndAppendIndex(messageByte.length, writeIndexFileBuffer);
		// 3. 放入阻塞队列
		if (offset % Constants.LOG_BUFFER_SIZE + messageByte.length <= Constants.LOG_BUFFER_SIZE) {
			GlobalResource.putWriteTask(new WriteTask(writeLogFileBuffer, messageByte, offset));
		} else { // 跨 buffer 的, 分为两个放入 Queue
			int size1 = (int) (Constants.LOG_BUFFER_SIZE - offset % Constants.LOG_BUFFER_SIZE);
			byte[] part1 = new byte[size1], part2 = new byte[messageByte.length - size1];
			System.arraycopy(messageByte, 0, part1, 0, size1);
			System.arraycopy(messageByte, size1, part2, 0, part2.length);
			GlobalResource.putWriteTask(new WriteTask(writeLogFileBuffer, part1, offset));
			GlobalResource.putWriteTask(new WriteTask(writeLogFileBuffer, part2, offset + size1));
		}
	}

	// for Consumer
	public List<PersistenceFile> getIndexFileList() {
		return indexFileList;
	}

	// for Consumer
	public List<PersistenceFile> getLogFileList() {
		return logFileList;
	}

	// for Consumer
	public long getNextIndexOffset() {
		return lastFile.getNextIndexOffset();
	}

	// for Producer, 由 GlobalResource.flush() 而来, 全局只会触发一次
	public void flush() throws InterruptedException {
		// 1. update lastIndex
		lastFile.flush();
		// 2. flush writeIndexFileBuffer
//		writeIndexFileBuffer.flush();
		// 3. flush writeLogFileBuffer
//		writeLogFileBuffer.flush();
	}

}
