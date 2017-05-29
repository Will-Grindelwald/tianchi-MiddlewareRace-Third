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
	private Boolean bufferClean;

	// buffer 二级缓存, 实现并发写
	private final byte[] bufferL2;
	private final int bufferL2Size;
	private final AtomicInteger bufferL2Count = new AtomicInteger(0);

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
			buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE, blockNumber.get() * Constants.BUFFER_SIZE,
					Constants.BUFFER_SIZE);
			buffer.position((int) (offset % Constants.BUFFER_SIZE));
			bufferClean = true;
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
	public boolean write(byte[] bytes) throws InterruptedException {
		synchronized (bufferClean) {
			while (!bufferClean) {
				bufferClean.wait();
			}
		}
		buffer.put(bytes);
		if (buffer.remaining() == 0) {
			bufferClean = false;
			// WriteMessageService 与 ReMapService 对 bufferL2 的同步
			synchronized (blockNumber) {
				bufferL2Count.set(0);
				blockNumber.incrementAndGet();
				blockNumber.notifyAll();
			}
			GlobalResource.BufferReMapExecPool.submit(new ReMapService(this));
		}
		return true;
	}

	/**
	 * 此 write 设计为并发写 bufferL2. 且不会出现 bytes[] 要跨 bufferL2 写入, 这由外部来保证.
	 * 若要写入的块非当前块, 则阻塞
	 */
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
		}
		return true;
	}

	public boolean reMap() throws IOException {
		// 1
		force();
		// 2
		BufferUtils.clean(buffer);
		// 3
		if (blockNumber.get() % 50 == 0) { // 换新文件
			PersistenceFile newFile = new PersistenceFile(fileList.get(0).path, fileID++, fileNamePrefix);
			fileList.add(newFile);
			mappedFileChannel = newFile.getFileChannel();
		}
		buffer = mappedFileChannel.map(FileChannel.MapMode.READ_WRITE, blockNumber.get() * Constants.BUFFER_SIZE,
				Constants.BUFFER_SIZE);
		synchronized (bufferClean) {
			bufferClean = true;
			bufferClean.notifyAll();
		}
		return false;
	}

	public void force() {
		buffer.force();
	}

	public long flush() {
		// TODO
		return 0;
	}

}

class ReMapService implements Runnable {
	private WriteBuffer writeBuffer;

	public ReMapService(WriteBuffer writeBuffer) {
		this.writeBuffer = writeBuffer;
	}

	@Override
	public void run() {
		try {
			writeBuffer.reMap();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
