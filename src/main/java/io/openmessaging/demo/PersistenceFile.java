package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class PersistenceFile {
	public final String path;
	public final String fileName = Constants.LOG_FILE_NAME;

	private final RandomAccessFile file;
	private final FileChannel fileChannel;

	public PersistenceFile(String path) {
		this.path = path;
		File file = new File(path, fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
			fileChannel = this.file.getChannel();
		} catch (IOException e) {
			e.printStackTrace();
			throw new ClientOMSException("PersistenceFile create failure", e);
		}
	}

	public FileChannel getFileChannel() {
		return this.fileChannel;
	}

}
