package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class PersistenceFile {
	public final String path;
	public final int fileID;
	public final String fileNamePrefix;
	public final String fileName;

	private final RandomAccessFile file;
	private final FileChannel fileChannel;

	public PersistenceFile(String path, int fileID, String fileNamePrefix) {
		this.path = path;
		this.fileID = fileID;
		this.fileNamePrefix = fileNamePrefix;
		fileName = fileNamePrefix + fileID;
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
