package com.kenshin.search.core.index.manager;

import java.io.IOException;
import java.lang.Character.UnicodeScript;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.kenshin.search.core.index.CoreIndexer;
import com.kenshin.search.core.index.RamIndexer;
import com.kenshin.search.core.index.SegIndexer;
import com.kenshin.search.core.model.Model;
import com.kenshin.search.core.model.directory.CoreDirectoryDetail;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.resource.ResourcePool;

public class IndexerManager {
	
	private static final Logger logger = Logger.getLogger(IndexerManager.class);
	
	private final Analyzer analyzer = new StandardAnalyzer(); //分词器
	private final String INDEXPATH = "/home/kenshin/indexs/core/";
	private final String SEGPATH = "/home/kenshin/indexs/segs/";
	
	//资源池
	private final ResourcePool resourcePool;
	
	//默认启动数
	private final int MAX_RAM_INDEXER = Runtime.getRuntime().availableProcessors() + 1; //启动索引者数量
	private final ExecutorService ramIndexerPool = Executors.newFixedThreadPool(MAX_RAM_INDEXER);
	
	private final int MAX_SEG_INDEXER = 2 * Runtime.getRuntime().availableProcessors(); //启动索引者数量
	private final ExecutorService segIndexerPool = Executors.newFixedThreadPool(MAX_SEG_INDEXER);
	
	//统计已经做了多少条ram索引
	private static final AtomicInteger count = new AtomicInteger(0);
	
	public IndexerManager(ResourcePool resourcePool) {
		super();
		this.resourcePool = resourcePool;
	}
	
	public void start() {
		//启动ram
		for(int i = 0; i < MAX_RAM_INDEXER; i++) {
			RamIndexer indexer;
			try {
				indexer = new RamIndexer("RamIndexer_" + i, analyzer, this, count);
				ramIndexerPool.submit(indexer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//启动seg
		for(int i = 0; i < MAX_SEG_INDEXER; i++) {
			SegIndexer indexer;
			indexer = new SegIndexer("SegIndexer_" + i, analyzer, this, SEGPATH);
			segIndexerPool.submit(indexer);
		}
		
		//启动core
		try {
			//本身初始化的时候会带有定时任务
			CoreIndexer coreIndexer = new CoreIndexer("CoreIndexer_" + 1, analyzer, this, INDEXPATH);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//启动监测
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<< ramIndexe has done " + count.get() + 
						" | orgigin data " + resourcePool.getOriginData().size());
			}
		}, 1, 5, TimeUnit.SECONDS);
		
		logger.info("<<<<<<<<<<<<<<<<<<< all indexer have started");
	}
	/**************************************** indexer回调函数push ***************************************/
	
	/**
	 * indexer推送需要写到seg中的ramDirectory
	 */
	public void pushReadyForSeg(RAMDirectoryDetail directoryDetail) {
		resourcePool.pushReadyForSeg(directoryDetail);
	}
	
	/**
	 * indexer推送更新的ramDirectory
	 */
	public void pushUpdateRam(RAMDirectoryDetail directory) {
		resourcePool.pushUpdateRam(directory);
	}
	
	/**
	 * segIndexer推送segDirectory
	 */
	public void pushReadyForCore(SegDirectoryDetail directoryDetail) {
		resourcePool.pushReadyForCore(directoryDetail);
	}
	
	/**
	 * 
	 */
	public void pushCoreDetail(CoreDirectoryDetail coreDirectoryDetail) {
		resourcePool.setCoreDirectoryDetail(coreDirectoryDetail);
	}
	
	/**
	 * coreIndexer告知manger准备开始merge
	 */
//	public Queue<Directory> startMergeCore() {
//		Queue<Directory> tmpData = segData;
//		segData = new LinkedBlockingQueue<Directory>();
//		return tmpData;
//	}
	
	/************************************** indexer回调函数take ************************************************/
	public Model takeOriginData() {
		return resourcePool.takeOriginData();
	}
	
	public RAMDirectoryDetail takeToSeg() {
		return resourcePool.takeToSeg();
	}
	
	public SegDirectoryDetail takeToCore() {
		return resourcePool.takeToCore();
	}
	
	public Queue<SegDirectoryDetail> getAllSeg() {
		return resourcePool.getAllSeg();
	}
	
}
