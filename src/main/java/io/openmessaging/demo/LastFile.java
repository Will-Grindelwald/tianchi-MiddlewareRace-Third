package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LastFile {
	private final String path;
	private final String fileName;
	private final RandomAccessFile lastFile;

	public Index lastIndex;

	public LastFile(String path, String fileName) {
		this.path = path;
		this.fileName = fileName;
		File last = new File(path, fileName);
		try {
			if (!last.exists()) {
				last.createNewFile();
				lastFile = new RandomAccessFile(last, "rw");
				lastIndex = new Index(0, 0, 0);
			} else {
				lastFile = new RandomAccessFile(last, "rw");
				if (last.length() != Constants.INDEX_SIZE) {
					lastIndex = new Index(0, 0, 0);
				} else {
					int fileID = lastFile.readInt();
					int offset = lastFile.readInt();
					int size = lastFile.readInt();
					lastIndex = new Index(fileID, offset, size);
				}
			}
		} catch (IOException e) {
			throw new ClientOMSException("Last create failure", e);
		}
	}

	public void flush(Index lastIndexOfProducer) {
		if (lastIndex.fileID < lastIndexOfProducer.fileID || (lastIndex.fileID == lastIndexOfProducer.fileID && lastIndex.offset < lastIndexOfProducer.offset)) {
			lastIndex = lastIndexOfProducer;
			try {
				lastFile.seek(0);
				lastFile.writeInt(lastIndex.fileID);
				lastFile.writeInt(lastIndex.offset);
				lastFile.writeInt(lastIndex.size);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
