package com.kenshin.search.core.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class ModelEventFactory implements EventFactory<ModelEvent> {

	public ModelEvent newInstance() {
		return new ModelEvent();
	}
}
