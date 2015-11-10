package com.kenshin.search.core.indexer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.kenshin.search.core.model.directory.CoreDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.resource.ResourcePool;

public class CoreIndexer extends AbstractIndexer {
	
	private static final Logger logger = Logger.getLogger(CoreIndexer.class);
	
	private static final int perTime = 30; //间隔时间
	
	private final Directory coreDirectory; //coreIndex的地址
	private final ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
	
	public CoreIndexer(Analyzer analyzer, ResourcePool resourcePool, String indexPath) throws IOException {
		super("coreIndexer", analyzer, resourcePool, 1, 1);
		this.coreDirectory = FSDirectory.open(Paths.get(indexPath));
		
		pool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					indexAll();
				} catch (IOException e) {
					logger.error("<<<<<<<<<<<<<<<<<<<< ini coreIndexer error : " + e.getMessage());
				}
			}
		}, 1, perTime, TimeUnit.SECONDS);
	}
	
	//将碎片文件索引到总文件里
	private void indexAll() throws IOException {
		
		Queue<SegDirectoryDetail> directoryDetails = resourcePool.getAllSeg();
		
		logger.debug("<<<<<<<<<<<<<<<<<<<< ini coreIndexer indexAll directoryDetails : " + directoryDetails.size());
		
		if(directoryDetails == null || directoryDetails.size() == 0)
			return;
		
		Queue<Directory> segDirectories = new LinkedList<Directory>();
		for(SegDirectoryDetail segDirectoryDetail : directoryDetails) {
			segDirectories.add(segDirectoryDetail.getDirectory());
		}
		
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter w = new IndexWriter(coreDirectory, config);
		
		w.addIndexes(segDirectories.toArray(new Directory[0]));
		w.commit();
		w.close();
		
		CoreDirectoryDetail coreDirectoryDetail = new CoreDirectoryDetail(indexName, coreDirectory, directoryDetails);
		
		resourcePool.setCoreDirectoryDetail(coreDirectoryDetail);
	}

}
