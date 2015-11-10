package com.kenshin.search.core.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class RAMDirectoryEventFactory implements EventFactory<RAMDirectoryEvent> {

	public RAMDirectoryEvent newInstance() {
		return new RAMDirectoryEvent();
	}
}
