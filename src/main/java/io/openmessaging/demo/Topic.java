package io.openmessaging.demo;

import java.io.File;

// 一个 buchet 一个, 全局唯一, 小心并发
public class Topic {

	private final String path;
	public final String bucket;
	public final int ID;

	// Last file
	private final LastFile lastFile;

	// LogFiles
	private final PersistenceFile logFile;
	private final WriteBuffer writeBuffer;

	public Topic(String bucket, int ID) {
		this.bucket = bucket;
		this.ID = ID;
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
		// LogFile
		logFile = new PersistenceFile(path);
		writeBuffer = new WriteBuffer(logFile, lastFile);
	}

	public WriteBuffer getWriteLogFileBuffer() {
		return writeBuffer;
	}

	// for Consumer
	public PersistenceFile getLogFileList() {
		return logFile;
	}

	// for Producer, 由 GlobalResource.flush() 而来, 全局只会触发一次
	public void flush() throws InterruptedException {
		writeBuffer.flush();
	}

}
