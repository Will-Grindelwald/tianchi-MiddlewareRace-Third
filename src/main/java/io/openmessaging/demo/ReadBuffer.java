package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * READ ONLY MappedByteBuffer Wrapper
 * for Consumer
 */
public class ReadBuffer {

	public String bucket = "";
	public FileChannel mappedFileChannel;
	public MappedByteBuffer buffer;
	public int size;
	public int offsetInFile;

	public ReadBuffer() {
		// do nothing
	}

	public void reMap(String bucket, FileChannel fileChannel, int offset, int size) {
		this.bucket = bucket;
		try {
			this.mappedFileChannel = fileChannel;
			this.size = (int) (offset + size < fileChannel.size() ? size : fileChannel.size());
			this.offsetInFile = offset + this.size;
			buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, this.size);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * return false when no more file content to map. that is to say:
	 * mappedFileChannel.size() = offsetInFile
	 */
	public boolean reMap() {
		return reMap(offsetInFile, size);
	}

	/**
	 * return false when no more file content to map. that is to say:
	 * mappedFileChannel.size() = offsetInFile
	 */
	public boolean reMap(int offset, int size) {
		try {
			if (mappedFileChannel.size() - offset < size) {
				size = (int) (mappedFileChannel.size() - offset);
			}
			if (size != 0) {
				buffer = mappedFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, size);
				offsetInFile = offset + size;
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 
	 * @param bucket bucket's name
	 * @param fileChannel the fileChannel of bucket
	 * @param offset 
	 * @param lenth
	 * @return
	 */
	public byte[] read(String bucket, FileChannel fileChannel, int offset, int lenth) {
		byte[] result = new byte[lenth];
		// readIndexFileBuffer 缓存不命中
		if (!bucket.equals(this.bucket)) {
			// 1. 不是同一个文件
			reMap(bucket, fileChannel, offset, Constants.BUFFER_SIZE);
			// TODO 测试 load 与 不 load 谁快
			// buffer.load();
			// TODO 边界用 >= ? 待测
		} else if (offset >= offsetInFile) {
			// 2. 超出映射范围
			reMap(offset, Constants.BUFFER_SIZE);
			// buffer.load();
		}

		// TODO 边界用 >= ? 待测
		if (lenth >= offsetInFile - offset) {
			// 读两段
			int size1 = (int) (offsetInFile - offset);
			buffer.get(result, 0, size1);
			if (!reMap()) {
				return null; // no more to map == no more new record
			}
			buffer.get(result, size1, lenth - size1);
		} else {
			// other, 正常读
			buffer.get(result);
		}
		return result;
	}
}
