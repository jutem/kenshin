package com.kenshin.search.core.resource;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.springframework.stereotype.Component;

import com.kenshin.search.core.disruptor.DisruptorFactory;
import com.kenshin.search.core.disruptor.event.ModelEvent;
import com.kenshin.search.core.disruptor.event.ModelEventFactory;
import com.kenshin.search.core.disruptor.event.RAMDirectoryEvent;
import com.kenshin.search.core.disruptor.event.RAMDirectoryEventFactory;
import com.kenshin.search.core.disruptor.event.SegDirectoryEvent;
import com.kenshin.search.core.disruptor.event.SegDirectoryEventFactory;
import com.kenshin.search.core.disruptor.producer.ModelEventProducerWithTranslator;
import com.kenshin.search.core.disruptor.producer.RAMDirectoryEventProducerWithTranslator;
import com.kenshin.search.core.disruptor.producer.SegDirectoryEventProducerWithTranslator;
import com.kenshin.search.core.indexer.CoreIndexer;
import com.kenshin.search.core.indexer.RamIndexer;
import com.kenshin.search.core.indexer.SegIndexer;
import com.kenshin.search.core.model.directory.CoreDirectoryDetail;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.reader.CoreReader;
import com.kenshin.search.core.reader.RAMReader;
import com.kenshin.search.core.reader.SegReader;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * 资源池
 * ram的同一个directory是有可能同时出现updateData和ramData中
 */
@Component
public class ResourcePool {
	
	private static final Logger logger = Logger.getLogger(ResourcePool.class);
	
	private final AtomicInteger count = new AtomicInteger(0);
	
	private Disruptor<ModelEvent> originData; //原始数据 生产者:用户数据 消费者:ramIndexer
	private Disruptor<RAMDirectoryEvent> updateData; //更新的数据，需要通知过滤 生产者:ramIndexer 消费者:readerManager
	
	private Map<Long, RAMDirectoryDetail> readyForSeg = new ConcurrentHashMap<Long, RAMDirectoryDetail>(); //准备写入到seg的数据 key:directoryId, value:DirectoryDetail
	private Disruptor<RAMDirectoryEvent> toSeg; //准备好写入seg 生产者:ramReader 消费者:segIndexer
	
	private Disruptor<SegDirectoryEvent> readyForCore; //准备合并到core的数据 生产者:segIndexer 消费者:segReader
	private Queue<SegDirectoryDetail> toCore = new ConcurrentLinkedQueue<SegDirectoryDetail>(); //准备好写入core 生产者:segReader 消费者coreIndexer
	
	private CoreDirectoryDetail coreDirectoryDetail = null; 
	
	//生产者
	private ModelEventProducerWithTranslator originProducer;
	private RAMDirectoryEventProducerWithTranslator updateDataProducer;
	private RAMDirectoryEventProducerWithTranslator toSegProducer;
	private SegDirectoryEventProducerWithTranslator readyForCoreProducer;
	
	//默认analyzer
	private static final Analyzer DEFAULT_ANALYZER = new StandardAnalyzer();
	
	//seg路径
	private final String SEGPATH = "/home/kenshin/indexs/segs/";
	private final String COREPATH = "/home/kenshin/indexs/core/";
	
	@Resource
	private ReaderManager readerManager;
	
	@PostConstruct
	public void init() {
		
		logger.info("<<<<<<<<<<<<<<<<<<<<<< cpu num : " + Runtime.getRuntime().availableProcessors());
		
		originData = DisruptorFactory.creatorCPUBusy(new ModelEventFactory());
		DisruptorFactory.withHandler(originData, RamIndexer.class, DisruptorFactory.RAMINDEXER, DEFAULT_ANALYZER, this, DisruptorFactory.CPUBUSY, null, count);
		originData.start();
		originProducer = new ModelEventProducerWithTranslator(originData.getRingBuffer());
		
		updateData = DisruptorFactory.creatorCPUBusy(new RAMDirectoryEventFactory());
		DisruptorFactory.withHandler(updateData, RAMReader.class, DisruptorFactory.RAMREADER, DEFAULT_ANALYZER, this, DisruptorFactory.CPUBUSY, readerManager);
		updateData.start();
		updateDataProducer = new RAMDirectoryEventProducerWithTranslator(updateData.getRingBuffer());
		
		toSeg = DisruptorFactory.creatorIOBusy(new RAMDirectoryEventFactory());
		DisruptorFactory.withHandler(toSeg, SegIndexer.class, DisruptorFactory.SEGINDEXER, DEFAULT_ANALYZER, this, DisruptorFactory.IOBUSY, null, SEGPATH);
		toSeg.start();
		toSegProducer = new RAMDirectoryEventProducerWithTranslator(toSeg.getRingBuffer());
		
		readyForCore = DisruptorFactory.creatorIOBusy(new SegDirectoryEventFactory());
		DisruptorFactory.withHandler(readyForCore, SegReader.class, DisruptorFactory.SEGREADER, DEFAULT_ANALYZER, this, DisruptorFactory.IOBUSY, readerManager);
		readyForCore.start();
		readyForCoreProducer = new SegDirectoryEventProducerWithTranslator(readyForCore.getRingBuffer());
		
		try {
			CoreIndexer coreIndexer = new CoreIndexer(DEFAULT_ANALYZER, this, COREPATH);
			CoreReader coreReader = new CoreReader(this, readerManager);
		} catch(Exception e) {
			logger.error("<<<<<<<<<<<<<<<<<<<< ini coreIndexer error : " + e.getMessage());
		}
		
		//启动监测
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<< disruptor ramIndexe has done " + count.get());
			}
		}, 1, 5, TimeUnit.SECONDS);
	}
	
	/********************************** 回调方法 **************************************/
	
	/**
	 * indexer推送需要写到seg中的ramDirectory
	 */
	public void pushReadyForSeg(RAMDirectoryDetail directory) {
		logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in ready for seg : " + directory.getId());
		readyForSeg.put(directory.getId(), directory);
	}
	
	/**
	 * 替换core
	 */
	public void setCoreDirectoryDetail(CoreDirectoryDetail coreDirectoryDetail) {
		if(coreDirectoryDetail != null) {
			logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in coreDetail : " + coreDirectoryDetail.getId());
		}
		this.coreDirectoryDetail = coreDirectoryDetail;
	}

	public CoreDirectoryDetail getCoreDirectoryDetail() {
		return coreDirectoryDetail;
	}
	
	public Queue<SegDirectoryDetail> getAllSeg() {
		Queue<SegDirectoryDetail> tmpQueue = toCore;
		toCore = new ConcurrentLinkedQueue<SegDirectoryDetail>();
		return tmpQueue;
	}
	
	public Queue<SegDirectoryDetail> getToCore() {
		return toCore;
	}
	
	/********************************** reader回调 ***************************************/
	
	/**
	 * 解锁这块directory可以进入seg
	 */
	public void unLockDirectoryDetail(long directoryId) {
		RAMDirectoryDetail ramDirectoryDetail = readyForSeg.remove(directoryId);
		if(ramDirectoryDetail != null) {
			logger.debug("<<<<<<<<<<<<<<<<<<<<<<< directory in to seg : " + directoryId);
			ramDirectoryDetail.setReady(true);
			toSegProducer.onData(ramDirectoryDetail);
		}
	}

	/******************************* 返回producer ***************************************/
	public ModelEventProducerWithTranslator getOriginProducer() {
		return originProducer;
	}

	public RAMDirectoryEventProducerWithTranslator getUpdateDataProducer() {
		return updateDataProducer;
	}

	public RAMDirectoryEventProducerWithTranslator getToSegProducer() {
		return toSegProducer;
	}
	
	public SegDirectoryEventProducerWithTranslator getReadyForCoreProducer() {
		return readyForCoreProducer;
	}
	
}
