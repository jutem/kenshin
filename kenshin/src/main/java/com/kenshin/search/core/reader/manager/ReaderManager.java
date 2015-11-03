package com.kenshin.search.core.reader.manager;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;

import com.kenshin.search.core.model.directory.CoreDirectoryDetail;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.reader.filter.IFilterDirectoryReader;
import com.kenshin.search.core.reader.filter.IFilterDirectoryReader.ISubReaderWrapper;
import com.kenshin.search.core.reader.query.CommonQuery;
import com.kenshin.search.core.reader.reader.CommonSearcher;
import com.kenshin.search.core.resource.ResourcePool;

public class ReaderManager {
	
	private static final Logger logger = Logger.getLogger(ReaderManager.class);
	
	private final Analyzer analyzer = new StandardAnalyzer(); //分词器
	
	private DirectoryReader coreReader = null; //coreIndexReader
	private Map<Long, DirectoryReader> segDirectoryReaders = new ConcurrentHashMap<Long, DirectoryReader>(); //segReader key:directoryId value:segDirectoryReader
	private Map<String, DirectoryReader> ramDirectoryReaders = new ConcurrentHashMap<String, DirectoryReader>(); //ramReader key:indexerName value:ramDirectoryReader
	
	private Map<String, Boolean> ramModelIdMap = new ConcurrentHashMap<String, Boolean>(); //ram中存在的modelId, 用来过滤
	private Map<String, Boolean> segModelIdMap = new ConcurrentHashMap<String, Boolean>(); //seg中存在的modelId， 用来过滤
	
	private final ResourcePool resourcePool;
	
	//默认启动数
	private static final int MAX_RAM_READER = 10; //启动open ramdirectory
	private static final ExecutorService ramReaderPool = Executors.newFixedThreadPool(MAX_RAM_READER);
		
	private static final int MAX_SEG_READER = 10; //启动open segdirectory
	private static final ExecutorService segReaderPool = Executors.newFixedThreadPool(MAX_SEG_READER);
	
	private final ExecutorService coreReaderPool = Executors.newSingleThreadExecutor();
	
	//同时支持的查询线程池
//	private final ExecutorService searcherPool = Executors.newCachedThreadPool();
	
	public ReaderManager(ResourcePool resourcePool) {
		super();
		this.resourcePool = resourcePool;
	}
	
	public void start() {
		//启动ram
		for(int i = 0; i < MAX_RAM_READER; i++) {
			ramReaderPool.submit(new Runnable() {
				@Override
				public void run() {
					reOpenRamReader();
				}
			});
		}
		
		//启动seg
		for(int i = 0; i < MAX_SEG_READER; i++) {
			segReaderPool.submit(new Runnable() {
				@Override
				public void run() {
					openSegReader();
				}
			});
		}
		
		coreReaderPool.submit(new Runnable() {
			@Override
			public void run() {
				reOpenCoreReader();
			}
		});
		
		logger.info("<<<<<<<<<<<<<<<<<<< all readOpen have started");
	}
	
	/**
	 * 重新打开最新的ramDirectory
	 */
	public void reOpenRamReader() {
		while(true) {
			try {
				RAMDirectoryDetail directoryDetail = resourcePool.takeUpdateDirectoryDetail();
				Directory directory = directoryDetail.getDirectory();
				DirectoryReader directoryReader = DirectoryReader.open(directory);
				
				Queue<String> modelIds = directoryDetail.getModelIds();
//				String modelId = directoryDetail.getModel().getId();
				
				//TODO需要加锁
				ramDirectoryReaders.put(directoryDetail.getIndexerName(), directoryReader);
				for(String modelId : modelIds) {
					ramModelIdMap.put(modelId, true);
				}
				
				//通知已经可以解锁这个seg了
				resourcePool.unLockDirectoryDetail(directoryDetail.getId());
				
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 打开最新的seg
	 */
	public void openSegReader() {
		while(true) {
			try {
				SegDirectoryDetail directoryDetail = resourcePool.takeReadyForCore();
				Directory directory = directoryDetail.getDirectory();
				DirectoryReader directoryReader = DirectoryReader.open(directory);
				
				//TODO需要加锁
				ramDirectoryReaders.remove(directoryDetail.getRamIndexerName()); //可以清理这个directory了
				segDirectoryReaders.put(directoryDetail.getId(), directoryReader);
				
				/**
				 * TODO
				 * 解决当新的更新文档进来，但是这里却把他删除了的问题
				 */
				for(String modelId : directoryDetail.getModelIds()) {
					ramModelIdMap.remove(modelId);
					segModelIdMap.put(modelId, true);
				}
				
				//通知这个directory已经可以进入seg
				resourcePool.toCore(directoryDetail);
				
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 重新打开CoreReader
	 */
	public void reOpenCoreReader() {
		while(true) {
			try {
				//重新打开core耗时很长
				CoreDirectoryDetail directoryDetail = resourcePool.getCoreDirectoryDetail();
				if(directoryDetail == null) {
					Thread.sleep(60 * 1000);
					continue;
				}
				Directory directory = directoryDetail.getDirectory();
				DirectoryReader directoryReader = DirectoryReader.open(directory);
				
				Queue<SegDirectoryDetail> segDirectoryDetails = directoryDetail.getSegDirectoryDetails();
				
				//TODO需要加锁
				coreReader = directoryReader;
				for(SegDirectoryDetail segDirectoryDetail : segDirectoryDetails) {
					segDirectoryReaders.remove(segDirectoryDetail.getId());
				}
				
				resourcePool.setCoreDirectoryDetail(null);
				
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/*************************** 提供查询功能 ******************************************/
	public List<Document> CommonQuery(CommonQuery commonQuery, int hitsPerPage) throws ParseException, IOException {
		
		Query q = MultiFieldQueryParser.parse(commonQuery.getQueries(), commonQuery.getFields(), analyzer);
		
		List<DirectoryReader> readers = sortCommonReaders();
		
		CommonSearcher commonSearcher = new CommonSearcher(q, readers, hitsPerPage);
		return commonSearcher.query();
	}
	
	private List<DirectoryReader> sortCommonReaders() throws IOException {
		List<DirectoryReader> readers = new LinkedList<DirectoryReader>();
		//RAMReaders
		readers.addAll(ramDirectoryReaders.values());
		
		//segReaders
		for(Map.Entry<Long, DirectoryReader> entry : segDirectoryReaders.entrySet()) {
			DirectoryReader segFilterReader = new IFilterDirectoryReader(entry.getValue(), new ISubReaderWrapper(ramModelIdMap));
			if(segFilterReader != null) {
				readers.add(segFilterReader);
			}
		}
		//coreReader
		Map<String, Boolean> tmpModelIdMap = new LinkedHashMap<String, Boolean>();
		tmpModelIdMap.putAll(ramModelIdMap);
		tmpModelIdMap.putAll(segModelIdMap);
		DirectoryReader coreFilterReader = new IFilterDirectoryReader(coreReader, new ISubReaderWrapper(tmpModelIdMap));
		
		if(coreFilterReader != null)
			readers.add(coreFilterReader);
		
		return readers;
	}
	
	
	/************************************** 测试代码 *******************************************/
//	ramModelIdMap.put("1", true);
//	logger.debug("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ramModelIdMap is : " + ramModelIdMap.size() + " map : " + ramModelIdMap.get("1"));
//	for(Map.Entry<String, DirectoryReader> entry : ramDirectoryReaders.entrySet()) {
//		logger.debug("<<<<<<<<<<<<<<<<<<<<<<<<<< entry value :" + entry.getValue());
//		DirectoryReader ramFilterReader = new IFilterDirectoryReader(entry.getValue(), new ISubReaderWrapper(ramModelIdMap));
////		IFilterDirectoryReader ramFilterReader = new IFilterDirectoryReader(entry.getValue(), new ISubReaderWrapper(ramModelIdMap));
////		DirectoryReader test = ramFilterReader.doWrapDirectoryReader(entry.getValue(), ramModelIdMap);
//		
//		if(ramFilterReader != null) {
//			readers.add(ramFilterReader); 
//		}
//	}
	
}
