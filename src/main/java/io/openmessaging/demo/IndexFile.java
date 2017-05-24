package io.openmessaging.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile {
	// 一个读写锁
	private static ReentrantLock fileWriteLock = new ReentrantLock();

	/*
	 * 索引文件结构：
	 * ----------------------------
	 * |fileName|offset|mesagesize|
	 * ----------------------------
	 */
	private String fileName; 
	private Long offset;
	private Long messageSize;
	
	private String path;
	private FileChannel fc;
	private MappedByteBuffer mappedByteBuffer;
	
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
	public void writeIndexFile(String fileName,long offset,long size){
		fileWriteLock.lock();
		
	}
	


	public byte[] readIndexByOffset(long offset) {
		return null;
	}
	
	public void flush() {

	}

}
