package com.kenshin.search.core.disruptor.producer;

import org.apache.log4j.Logger;

import com.kenshin.search.core.disruptor.event.SegDirectoryEvent;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

public class SegDirectoryEventProducerWithTranslator {
	private static final Logger logger = Logger.getLogger(SegDirectoryEventProducerWithTranslator.class);
	
	private final RingBuffer<SegDirectoryEvent> ringBuffer;

	public SegDirectoryEventProducerWithTranslator(RingBuffer<SegDirectoryEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	private static final EventTranslatorOneArg<SegDirectoryEvent, SegDirectoryDetail> TRANSLATOR = new EventTranslatorOneArg<SegDirectoryEvent, SegDirectoryDetail>() {
		public void translateTo(SegDirectoryEvent event, long sequence, SegDirectoryDetail directory) {
			event.setDirectory(directory);
		}
	};

	public void onData(SegDirectoryDetail directory) {
		ringBuffer.publishEvent(TRANSLATOR, directory);
	}
}     
