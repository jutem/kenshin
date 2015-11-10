package com.kenshin.search.core.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class SegDirectoryEventFactory implements EventFactory<SegDirectoryEvent> {

	public SegDirectoryEvent newInstance() {
		return new SegDirectoryEvent();
	}
}
