package com.kenshin.search.core.disruptor;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DisruptorFactory {
	
	private static final int MAX_RAM_INDEXER = Runtime.getRuntime().availableProcessors() + 1; //启动索引者数量
	private static final int DEFAULT_BUFFERSIZE = 1024 * 256;
	private static final Executor DEFAULT_EXECUTOR = Executors.newCachedThreadPool();
	
	@SuppressWarnings("unchecked")
	public static <T> Disruptor<T> creator(EventFactory<T> factory, EventHandler<T> handler, int bufferSize, Executor executor) {
        Disruptor<T> disruptor = new Disruptor<T>(factory, bufferSize, executor, ProducerType.SINGLE, new YieldingWaitStrategy());
        disruptor.handleEventsWith(handler);
        return disruptor;
	}
	
	public static <T> Disruptor<T> creator(EventFactory<T> factory, EventHandler<T> handler) {
		return creator(factory, handler, DEFAULT_BUFFERSIZE, DEFAULT_EXECUTOR);
	}
	
}
