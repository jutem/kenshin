package com.kenshin.search.core.reader.manager;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import com.kenshin.search.core.model.directory.CoreDirectoryDetail;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.resource.ResourcePool;

public class ReaderManager {
	
	private DirectoryReader coreReader = null; //coreIndexReader
	private Map<Long, DirectoryReader> segDirectoryReaders = new ConcurrentHashMap<Long, DirectoryReader>(); //segReader key:directoryId value:segDirectoryReader
	private Map<String, DirectoryReader> ramDirectoryReaders = new ConcurrentHashMap<String, DirectoryReader>(); //ramReader key:indexerName value:ramDirectoryReader
	
	private Map<String, Boolean> filterMap = new ConcurrentHashMap<String, Boolean>(); //过滤的map
	
	private final ResourcePool resourcePool;
	
	//默认启动数
	private static final int MAX_RAM_READER = 10; //启动open ramdirectory
	private static final ExecutorService ramReaderPool = Executors.newFixedThreadPool(MAX_RAM_READER);
		
	private static final int MAX_SEG_READER = 10; //启动索引者数量
	private static final ExecutorService segReaderPool = Executors.newFixedThreadPool(MAX_SEG_READER);
	
	private static final ExecutorService coreReaderPool = Executors.newSingleThreadExecutor();
	
	
	
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
		
		System.out.println("<<<<<<<<<<<<<<<<<<< all readOpen have started");
	}
	
	/**
	 * 重新打开最新的ramDirectory
	 */
	public void reOpenRamReader() {
		while(true) {
			try {
				RAMDirectoryDetail directoryDetail = resourcePool.takeUpdateDirectoryDetail();
				Directory directory = directoryDetail.getDirectory();
				DirectoryReader directoryReader =DirectoryReader.open(directory);
				
				Queue<String> modelIds = directoryDetail.getModelIds();
//				String modelId = directoryDetail.getModel().getId();
				
				//TODO需要加锁
				ramDirectoryReaders.put(directoryDetail.getIndexerName(), directoryReader);
				for(String modelId : modelIds) {
					filterMap.put(modelId, true);
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
					filterMap.remove(modelId);
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
	
}
