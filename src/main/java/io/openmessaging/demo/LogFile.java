package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

// 全局唯一, 小心并发
public class LogFile {
	private final String path;
	private final String fileName;
	private final RandomAccessFile file;
	private final FileChannel fileChannel;
	private MappedByteBuffer writeMappedByteBuffer;
	private AtomicInteger offset = new AtomicInteger(0);

	public LogFile(String path, String fileName) {
		this.path = path;
		this.fileName = fileName;
		File file = new File(path, fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			this.file = new RandomAccessFile(file, "rw");
			this.fileChannel = this.file.getChannel();
			this.writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constants.BUFFER_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ClientOMSException("LogFile create failure", e);
		}
	}

	// for Producer
	public void doAppend(byte[] bytes) {
		try {
			if (offset.get() > 4) {
				offset.set(0);
			}
			if(writeMappedByteBuffer.remaining()>=bytes.length){
				writeMappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset.get()*Constants.BUFFER_SIZE,
						Constants.BUFFER_SIZE);
				writeMappedByteBuffer.clear();

			}
			writeMappedByteBuffer.put(bytes);
			offset.incrementAndGet();
		} catch (IOException e) {
			e.printStackTrace();
		}
//		writeMappedByteBuffer.put(bytes,offset.updateAndGet(x->x>=3?)*Constants.BUFFER_SIZE,Constants.BUFFER_SIZE);
//		if(offset.get()>=3){
//			writeMappedByteBuffer.put(bytes,(offset.get()-3)*Constants.BUFFER_SIZE,Constants.BUFFER_SIZE);
//		}
//		else{
//			writeMappedByteBuffer.put(bytes,(offset.get())*Constants.BUFFER_SIZE,Constants.BUFFER_SIZE);
//		}

		
	}

	// for Consumer
	public String getFileName() {
		return this.fileName;
	}

	// for Consumer
	public FileChannel getFileChannel() {
		return this.fileChannel;
	}

	// for Producer
	public void flush() {
		try {
			writeMappedByteBuffer.force();
			fileChannel.force(false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
