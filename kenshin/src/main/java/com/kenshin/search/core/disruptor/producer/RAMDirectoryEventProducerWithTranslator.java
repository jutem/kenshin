package com.kenshin.search.core.disruptor.producer;

import org.apache.log4j.Logger;

import com.kenshin.search.core.disruptor.event.RAMDirectoryEvent;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

public class RAMDirectoryEventProducerWithTranslator {
	private static final Logger logger = Logger.getLogger(RAMDirectoryEventProducerWithTranslator.class);
	
	private final RingBuffer<RAMDirectoryEvent> ringBuffer;

	public RAMDirectoryEventProducerWithTranslator(RingBuffer<RAMDirectoryEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	private static final EventTranslatorOneArg<RAMDirectoryEvent, RAMDirectoryDetail> TRANSLATOR = new EventTranslatorOneArg<RAMDirectoryEvent, RAMDirectoryDetail>() {
		public void translateTo(RAMDirectoryEvent event, long sequence, RAMDirectoryDetail directory) {
			event.setDirectory(directory);
		}
	};

	public void onData(RAMDirectoryDetail directory) {
		ringBuffer.publishEvent(TRANSLATOR, directory);
	}
}     
