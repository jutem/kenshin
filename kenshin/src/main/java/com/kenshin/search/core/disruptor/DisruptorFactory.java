package com.kenshin.search.core.disruptor;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;

import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.ResourcePool;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DisruptorFactory {
	
	private static final Logger logger = Logger.getLogger(DisruptorFactory.class);
	
	public static final int CPUBUSY = Runtime.getRuntime().availableProcessors() + 1;
	public static final int IOBUSY = Runtime.getRuntime().availableProcessors() * 2;
	private static final int DEFAULT_BUFFERSIZE = 1024 * 256;
	
	public static final int RAMINDEXER = 0;
	public static final int SEGINDEXER = 1;
	public static final int COREINDEXER = 2;
	
	public static final int RAMREADER = 3;
	public static final int SEGREADER = 4;
	
	public static <T> Disruptor<T> creator(EventFactory<T> factory, int bufferSize, Executor executor) {
        Disruptor<T> disruptor = new Disruptor<T>(factory, bufferSize, executor, ProducerType.SINGLE, new YieldingWaitStrategy());
        return disruptor;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> Disruptor<T> creatorWithHandler(EventFactory<T> factory, EventHandler<T> handler) {
        Disruptor<T> disruptor = new Disruptor<T>(factory, DEFAULT_BUFFERSIZE, Executors.newSingleThreadExecutor(), ProducerType.SINGLE, new YieldingWaitStrategy());
        disruptor.handleEventsWith(handler);
        return disruptor;
	}
	
	public static <T> Disruptor<T> creatorCPUBusy(EventFactory<T> factory) {
		return creator(factory, DEFAULT_BUFFERSIZE, Executors.newFixedThreadPool(CPUBUSY));
	}
	
	public static <T> Disruptor<T> creatorIOBusy(EventFactory<T> factory) {
		return creator(factory, DEFAULT_BUFFERSIZE, Executors.newFixedThreadPool(IOBUSY));
	}
	
	public static <T, V> void withHandler(Disruptor<V> disruptor, Class<T> clazz, int type, 
			Analyzer analyzer, ResourcePool resourcePool, long numberOfConsumers, ReaderManager readerManager, Object ... other) {
		try {
			Constructor<T> construct = null;
			switch(type) {
			case RAMINDEXER:
				construct = clazz.getConstructor(Analyzer.class, ResourcePool.class, long.class, long.class, AtomicInteger.class);
				break;
			case SEGINDEXER:
				construct = clazz.getConstructor(Analyzer.class, ResourcePool.class, long.class, long.class, String.class);
				break;
			case COREINDEXER:
				construct = clazz.getConstructor(Analyzer.class, ResourcePool.class, long.class, long.class, String.class);
				break;
			case RAMREADER:
			case SEGREADER:
				construct = clazz.getConstructor(ResourcePool.class, ReaderManager.class, long.class, long.class);
			}
			
			List<T> tList = new LinkedList<T>();
			if(type > 2) {
				for(int i = 0; i < numberOfConsumers; i++) {
					Object[] parameters = wrapParameters(resourcePool, readerManager, i, numberOfConsumers, other);
					tList.add(construct.newInstance(parameters));
				}
			} else {
				for(int i = 0; i < numberOfConsumers; i++) {
					Object[] parameters = wrapParameters(analyzer, resourcePool, i, numberOfConsumers, other);
					tList.add(construct.newInstance(parameters));
				}
			}
			
			
			disruptor.handleEventsWith(tList.toArray(new EventHandler[0]));
		} catch(Exception e) {
			logger.info("<<<<<<<<<<<<<<<<<<< withHandler error : " + e.getMessage());
		}
	}
	
	/**
	 * 包装参数，这里顺序是有规定的
	 */
	public static Object[] wrapParameters(Analyzer analyzer, ResourcePool resourcePool, long ordinal, long numberOfConsumers, Object ... other) {
		List<Object> list = new LinkedList<Object>();
		list.add(analyzer);
		list.add(resourcePool);
		list.add(ordinal);
		list.add(numberOfConsumers);
		if(other != null) {
			for(Object o : other) {
				list.add(o);
			}
		}
		
		return list.toArray();
	}
	
	public static Object[] wrapParameters(ResourcePool resourcePool, ReaderManager readerManager, long ordinal, long numberOfConsumers, Object ... other) {
		List<Object> list = new LinkedList<Object>();
		list.add(resourcePool);
		list.add(readerManager);
		list.add(ordinal);
		list.add(numberOfConsumers);
		if(other != null) {
			for(Object o : other) {
				list.add(o);
			}
		}
		
		return list.toArray();
	}
	
}
