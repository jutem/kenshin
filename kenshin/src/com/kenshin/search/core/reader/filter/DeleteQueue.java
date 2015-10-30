package com.kenshin.search.core.reader.filter;

import java.util.concurrent.LinkedBlockingQueue;

import com.kenshin.search.core.model.Model;

public class DeleteQueue {
	private LinkedBlockingQueue<String> queue;
	//private static final int DELETE_QUEUE_CAPACITY = 1000;
	
	public DeleteQueue() {
		this.queue = new LinkedBlockingQueue<String>();
	}
	
	public void add(Model model) {
		try {
			this.queue.put(model.getId());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public LinkedBlockingQueue<String> getQueue() {
		return queue;
	}

}
