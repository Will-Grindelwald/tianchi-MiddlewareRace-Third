package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * READ ONLY MappedByteBuffer Wrapper
 * @author will
 *
 */
public class ReadBuffer {

	public FileChannel mappedFileChannel;
	public MappedByteBuffer buffer;
	public int size;
	public int offsetInFile;

	public ReadBuffer(FileChannel fileChannel, int size) {
		reMap(fileChannel, size);
	}

	public void reMap(FileChannel fileChannel, int size) {
		try {
			this.mappedFileChannel = fileChannel;
			this.offsetInFile = this.size = (int) (size < fileChannel.size() ? size : fileChannel.size());
			buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, this.size);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean reMap() {
		try {
			if (mappedFileChannel.size() - offsetInFile < size) {
				size = (int) (mappedFileChannel.size() - offsetInFile);
			}
			if (size != 0) {
				buffer = mappedFileChannel.map(FileChannel.MapMode.READ_ONLY, offsetInFile, size);
				offsetInFile += size;
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

}
