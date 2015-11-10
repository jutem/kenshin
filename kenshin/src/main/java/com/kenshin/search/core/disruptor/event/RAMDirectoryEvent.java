package com.kenshin.search.core.disruptor.event;

import com.kenshin.search.core.model.directory.RAMDirectoryDetail;

public class RAMDirectoryEvent {
	
	private RAMDirectoryDetail Directory;

	public RAMDirectoryDetail getDirectory() {
		return Directory;
	}

	public void setDirectory(RAMDirectoryDetail directory) {
		Directory = directory;
	}
	
}
