package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LastFile {
	private final String path;
	private final String fileName = Constants.LAST_FILE_NAME;
	private final RandomAccessFile lastFile;

	public Index lastIndex;
	public long lastIndexInIndexFile;

	public LastFile(String path) {
		this.path = path;
		File last = new File(path, fileName);
		try {
			if (!last.exists()) {
				last.createNewFile();
				lastFile = new RandomAccessFile(last, "rw");
				lastIndex = new Index(0, 0);
			} else {
				lastFile = new RandomAccessFile(last, "rw");
				if (last.length() != Constants.INDEX_SIZE) {
					lastIndex = new Index(0, 0);
				} else {
					long offset = lastFile.readLong();
					int size = lastFile.readInt();
					lastIndexInIndexFile = lastFile.readLong();
					lastIndex = new Index(offset, size);
				}
			}
		} catch (IOException e) {
			throw new ClientOMSException("Last create failure", e);
		}
	}

//	public synchronized void flush(long , int , long lastIndexOffset) {
//		if (lastIndexInIndexFile < lastIndexOffset) {
//			lastIndex = lastIndexOfProducer;
//			try {
//				lastFile.seek(0);
//				lastFile.writeLong(lastIndex.offset);
//				lastFile.writeInt(lastIndex.size);
//				lastFile.writeLong(lastIndexOffset);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}

}
