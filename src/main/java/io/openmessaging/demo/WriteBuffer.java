package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * READ WRITE MappedByteBuffer Wrapper for Producer
 */
public class WriteBuffer {

	private final String fileNamePrefix;

	private int fileID;
	// 当前映射块在整个 topic 中的 块号
	private final AtomicInteger blockNumber = new AtomicInteger(0);
	private final List<PersistenceFile> fileList;
	private FileChannel mappedFileChannel = null;
	private MappedByteBuffer buffer;
	private Boolean bufferNotFull;

	// buffer 二级缓存, 实现并发写
	private final byte[] bufferL2;
	// 同时标记是否带有二级缓存, indexFile buffer or logFile buffer
	private final int bufferL2Size;
	private final AtomicInteger bufferL2Count = new AtomicInteger(0);

	// for close
	private boolean close = false;

	public WriteBuffer(String fileNamePrefix, List<PersistenceFile> fileList, long offset, int bufferL2Size) {
		this.fileNamePrefix = fileNamePrefix;
		this.fileList = fileList;
		fileID = (int) (offset / Constants.FILE_SIZE);
		blockNumber.set((int) (offset / Constants.BUFFER_SIZE));
		for (PersistenceFile file : fileList) {
			if (file.fileID == fileID) {
				mappedFileChannel = file.getFileChannel();
				break;
			}
		}
		if (mappedFileChannel == null) {
			System.err.println("ERROR PersistenceFile 丢失");
			new Exception().printStackTrace();
			System.exit(0);
		}
		try {
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE,
					blockNumber.get() % Constants.BLOCK_NUMBER * Constants.BUFFER_SIZE, Constants.BUFFER_SIZE);
			buffer.position((int) (offset % Constants.BUFFER_SIZE));
			bufferNotFull = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.bufferL2Size = bufferL2Size;
		if (bufferL2Size != 0) {
			bufferL2 = new byte[bufferL2Size];
		} else {
			bufferL2 = null;
		}
	}

	/**
	 * 此 write 设计为顺序写 buffer. 且不会出现 bytes[] 要跨 buffer 写入, 这由外部来保证.
	 */
	// for indexFileBuffer
	public long write(byte[] bytes) throws InterruptedException {
		long ret = write(bytes, true);
		if (close) {
			buffer.force();
		}
		return ret;
	}

	/**
	 * 此 write 设计为顺序写 buffer. 且不会出现 bytes[] 要跨 buffer 写入, 这由外部来保证.
	 */
	public long write(byte[] bytes, boolean needReMap) throws InterruptedException {
		synchronized (bufferNotFull) {
			while (!bufferNotFull) {
				bufferNotFull.wait();
			}
		}
		synchronized (buffer) {
			buffer.put(bytes);
		}
		if (needReMap) {
			if (buffer.remaining() == 0) {
				bufferNotFull = false;
				if (bufferL2Size != 0) {
					bufferL2Count.set(0);
					// WriteMessageService 与 ReMapService 对 bufferL2 的同步
					synchronized (blockNumber) {
						blockNumber.incrementAndGet();
						blockNumber.notifyAll();
					}
				}
				GlobalResource.BufferReMapExecPool.submit(this::reMap);
			}
		} else { // for close state
			buffer.clear();
		}
		return (long) buffer.position() + blockNumber.get() * Constants.BUFFER_SIZE;
	}

	/**
	 * 此 write 设计为并发写 bufferL2. 且不会出现 bytes[] 要跨 bufferL2 写入, 这由外部来保证.
	 * 若要写入的块非当前块, 则阻塞
	 */
	// for logFileBuffer
	public boolean write(byte[] bytes, long offset) throws InterruptedException {
		if (bufferL2Size == 0)
			return false;
		int targetBlockNumber = (int) (offset / Constants.BUFFER_SIZE);
		synchronized (blockNumber) {
			while (targetBlockNumber != blockNumber.get()) {
				blockNumber.wait();
			}
		}
		System.arraycopy(bytes, 0, bufferL2, (int) (offset % Constants.BUFFER_SIZE), bytes.length);
		if (bufferL2Count.addAndGet(bytes.length) == bufferL2Size) {
			write(bufferL2);
		} else if (close) {
			write(bufferL2, false);
		}
		return true;
	}

	public void reMap() {
		try {
			System.out.println("c");
			// 1
			buffer.force();
			// 2
			BufferUtils.clean(buffer);
			// 3
			if (blockNumber.get() % Constants.BLOCK_NUMBER == 0) { // 换新文件
				PersistenceFile newFile = new PersistenceFile(fileList.get(0).path, ++fileID, fileNamePrefix);
				fileList.add(newFile);
				mappedFileChannel = newFile.getFileChannel();
			}
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE, blockNumber.get() * Constants.BUFFER_SIZE,
					Constants.BUFFER_SIZE);
			synchronized (bufferNotFull) {
				bufferNotFull = true;
				bufferNotFull.notifyAll();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void flush() {
		close = true;
		synchronized (buffer) {
			buffer.force();
		}
	}

}
