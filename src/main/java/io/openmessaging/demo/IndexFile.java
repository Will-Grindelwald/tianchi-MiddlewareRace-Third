package io.openmessaging.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile {
	// 一个读写锁
	private static ReentrantLock fileWriteLock = new ReentrantLock();
	private String path;
	private FileChannel fc;
	private ByteBuffer bf;//or Mapped
	public IndexFile(String path) {
		this.path = path+"/indexFile";
	}

	private void getChannel(){
		isExsits();
		try {
			fc=new RandomAccessFile(path, "rw").getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	private void isExsits() {
		File tmp=new File(path);
		if(!tmp.exists()){
			try {
				tmp.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public void writeIndexFile(){
		fileWriteLock.lock();
		
	}
	

//	public void creatFile() {
//		fileWriteLock.lock();
//		try {
//			file.createNewFile();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		finally{
//			fileWriteLock.unlock();
//		}
//	}

	public byte[] readIndexByOffset(long offset) {
		return null;
	}
	
	public void flush() {

	}

}
