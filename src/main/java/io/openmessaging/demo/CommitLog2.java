package io.openmessaging.demo;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CopyOnWriteArrayList;

// 一个 buchet 一个, 全局唯一, 小心并发
public class CommitLog2 {

	private final String path;
	private final LastFile lastFile;
	private final IndexFile indexFile;
	private final CopyOnWriteArrayList<LogFile2> logFileList = new CopyOnWriteArrayList<>();
	private LogFile2 lastLogFile;

	private MappedByteBuffer writeMappedByteBuffer;

	public CommitLog2(String path) {
		this.path = path;
		// topic dir
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + " 不是一个目录");
		} else {
			file.mkdirs();
		}
		// Last file
		lastFile = new LastFile(path, "Last");
		// indexFile
		indexFile = new IndexFile(path, "indexFile", lastFile.lastIndex.size);
		// LogFiles
		for (int i = 0; i < lastFile.lastIndex.fileID; i++) {
			logFileList.add(new LogFile2(path, i, -1));
		}
		lastLogFile = new LogFile2(path, lastFile.lastIndex.fileID, lastFile.lastIndex.offset);
		logFileList.add(lastLogFile);
	}

	// for Producer
	public Index appendIndex(int size) {
		return indexFile.appendIndex(size);
	}

	// for Producer
	public void appendMessage(byte[] messages, int logFileID, int offset) {
		if (lastLogFile.getFileID() != logFileID) {
			lastLogFile.flush();
			getNewLogFile();
		}
		lastLogFile.doAppend(messages, offset);
	}

	// for Producer
	public void getNewLogFile() {
		lastLogFile = new LogFile2(path, lastLogFile.getFileID() + 1, 0);
		logFileList.add(lastLogFile);
	}

	// for Consumer
	public FileChannel getIndexFileChannel() {
		return indexFile.getFileChannel();
	}

	// for Consumer
	public FileChannel getLogFileChannelByFileID(int fileID) {
		for (LogFile2 logFile : logFileList) {
			if (fileID == logFile.getFileID())
				return logFile.getFileChannel();
		}
		// twins loop for ...
		for (LogFile2 logFile : logFileList) {
			if (fileID == logFile.getFileID())
				return logFile.getFileChannel();
		}
		return null; // ERROR 文件丢失？
	}

	// for Producer
	// synchronized for lastFile
	public synchronized void flush(Index lastIndexOfProducer) {
		// 1. update lastIndex
		lastFile.flush(lastIndexOfProducer);
		// 2. flush Index

		// 3. flush LogFile

	}

}
