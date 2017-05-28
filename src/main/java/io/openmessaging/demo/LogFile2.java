package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

// 全局唯一, 小心并发
public class LogFile2 {
	private final String path;
	private final int fileID;
	private final RandomAccessFile file;
	private final FileChannel fileChannel;

	private MappedByteBuffer writeMappedByteBuffer;
	private boolean closeFlag = true;

	/**
	 * @param offset
	 *            if offset < 0, then don't initial writeMappedByteBuffer
	 */
	public LogFile2(String path, int fileID, int offset) {
		this.path = path;
		this.fileID = fileID;
		File file = new File(path, "LOG" + fileID);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
			fileChannel = this.file.getChannel();
			if (offset >= 0) {
				open(offset);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new ClientOMSException("LogFile create failure", e);
		}
	}

	public void open(int offset) throws IOException {
		if (closeFlag) {
			writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constants.LOG_FILE_SIZE);
			writeMappedByteBuffer.position(offset);
		}
		closeFlag = false;
	}

	public void close() throws IOException {
		if (!closeFlag) {
			writeMappedByteBuffer.force();
			fileChannel.force(false);
			clean(writeMappedByteBuffer);
		}
		closeFlag = true;
	}

	public boolean isClose() {
		return closeFlag;
	}

	// for Producer
	public void doAppend(byte[] message, int offsetSize) {
		if (writeMappedByteBuffer.position() != offsetSize) {
			writeMappedByteBuffer.position(offsetSize);
			System.out.println("重置 writeMappedByteBuffer position");
		}
		writeMappedByteBuffer.put(message);
	}

	// for Producer
	public int getFileID() {
		return this.fileID;
	}

	// for Consumer
	public FileChannel getFileChannel() {
		return this.fileChannel;
	}

	// for Producer
	public void flush() {
		try {
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// for Producer
	@SuppressWarnings("unchecked")
	public static void clean(final Object buffer) {
		AccessController.doPrivileged(new PrivilegedAction() {
			@SuppressWarnings("restriction")
			public Object run() {
				try {
					Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}

}
