package io.openmessaging.demo;

import java.io.File;

// 一个 buchet 一个, 全局唯一, 小心并发
public class Topic {

	private final String path;
	public final String bucket;
	public final int ID;

	private final PersistenceFile indexFile; // Index file
	private final PersistenceFile logFile; // Log File
	private final WriteBuffer writeBuffer; // Write Buffer

	public Topic(String bucket, int ID) {
		this.bucket = bucket;
		this.ID = ID;
		path = System.getProperty("path") + "/" + bucket;
		// topic dir
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory())
				throw new ClientOMSException(path + "不是一个目录");
		} else {
			file.mkdirs();
		}
		indexFile = new PersistenceFile(path, Constants.INDEX_FILE_NAME);
		logFile = new PersistenceFile(path, Constants.LOG_FILE_NAME);
		writeBuffer = new WriteBuffer(logFile, indexFile);
	}

	// for Producer
	public WriteBuffer getWriteBuffer() {
		return writeBuffer;
	}

	// for Consumer
	public PersistenceFile getLogFile() {
		return logFile;
	}

	// for Consumer
	public PersistenceFile getIndexFile() {
		return indexFile;
	}

}
