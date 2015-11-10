package com.kenshin.search.core.indexer;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.kenshin.search.core.disruptor.event.ModelEvent;
import com.kenshin.search.core.disruptor.event.RAMDirectoryEvent;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;
import com.kenshin.search.core.resource.DisruptorResourcePool;
import com.lmax.disruptor.EventHandler;

public class SegIndexer extends AbstractIndexer implements EventHandler<RAMDirectoryEvent>{
	
	private static final Logger logger = Logger.getLogger(SegIndexer.class);
	
	private final String segPath; //seg保存地址
	private Analyzer analyzer;
	
	public SegIndexer(String indexName, Analyzer analyzer, String segPath, DisruptorResourcePool resourcePool) {
		super(indexName, resourcePool);
		this.analyzer = analyzer;
		this.segPath = segPath;
	}
	
	// 将RAM写入文件
	private Directory indexFile(Directory directory)
			throws IOException {
		String fileName = segPath + indexName + "/" + indexName + "_" + System.currentTimeMillis();
		logger.debug("<<<<<<<<<<<<<<<< path is : " + fileName);
		logger.debug("<<<<<<<<<<<<<<<< segPath : " + segPath + " indexName " + indexName);
		//TODO fsd open可以预先生成
		Directory fsdDirectory = FSDirectory.open(Paths.get(fileName));
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter wf = new IndexWriter(fsdDirectory, config);
		wf.addIndexes(directory);
		wf.close();
		
		return fsdDirectory;
	}

	@Override
	public void onEvent(RAMDirectoryEvent event, long paramLong, boolean paramBoolean) throws Exception {
		RAMDirectoryDetail directoryDetail = event.getDirectory();
		Directory directory = directoryDetail.getDirectory();
		Directory fsdDirectory = indexFile(directory);
		
		SegDirectoryDetail segDirectoryDetail = new SegDirectoryDetail(indexName, directoryDetail.getIndexerName() ,fsdDirectory, directoryDetail.getModelIds()); 
		resourcePool.getReadyForCoreProducer().onData(segDirectoryDetail);
	}

}
