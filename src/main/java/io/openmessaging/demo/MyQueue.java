package io.openmessaging.demo;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class MyQueue<E> {
	private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<>();
	private Semaphore semaphore = new Semaphore(0);

	public E take() {
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return queue.poll();
	}

	public boolean add(E e) {
		boolean ret = queue.add(e);
		semaphore.release();
		return ret;
	}
}
