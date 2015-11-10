package com.kenshin.search.core.disruptor.event;

import com.kenshin.search.core.model.directory.SegDirectoryDetail;

public class SegDirectoryEvent {
	
	private SegDirectoryDetail Directory;

	public SegDirectoryDetail getDirectory() {
		return Directory;
	}

	public void setDirectory(SegDirectoryDetail directory) {
		Directory = directory;
	}
	
}
