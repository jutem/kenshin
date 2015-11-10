package com.kenshin.search.core.disruptor.producer;

import org.apache.log4j.Logger;

import com.kenshin.search.core.disruptor.event.ModelEvent;
import com.kenshin.search.core.model.Model;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

public class ModelEventProducerWithTranslator {
	private static final Logger logger = Logger.getLogger(ModelEventProducerWithTranslator.class);
	
	private final RingBuffer<ModelEvent> ringBuffer;

	public ModelEventProducerWithTranslator(RingBuffer<ModelEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	private static final EventTranslatorOneArg<ModelEvent, Model> TRANSLATOR = new EventTranslatorOneArg<ModelEvent, Model>() {
		public void translateTo(ModelEvent event, long sequence, Model model) {
			event.setModel(model);
		}
	};

	public void onData(Model model) {
		ringBuffer.publishEvent(TRANSLATOR, model);
	}
}     
