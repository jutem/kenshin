package com.kenshin.search.core.index;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.kenshin.search.core.index.manager.IndexerManager;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;
import com.kenshin.search.core.model.directory.SegDirectoryDetail;

public class SegIndexer extends AbstractIndexer implements Runnable{
	
	private static final Logger logger = Logger.getLogger(SegIndexer.class);
	
	private final String segPath; //seg保存地址
	private Analyzer analyzer;
	
	public SegIndexer(String indexName, Analyzer analyzer, IndexerManager indexerManager, String segPath) {
		super(indexName, indexerManager);
		this.analyzer = analyzer;
		this.segPath = segPath;
	}
	
	public void run() {
		while (true) {
			// 只读状态，说明在将索引写入硬盘
			try {
				RAMDirectoryDetail directoryDetail = indexerManager.takeToSeg();
				Directory directory = directoryDetail.getDirectory();
				Directory fsdDirectory = indexFile(directory);
				
				SegDirectoryDetail segDirectoryDetail = new SegDirectoryDetail(indexName, directoryDetail.getIndexerName() ,fsdDirectory, directoryDetail.getModelIds()); 
				indexerManager.pushReadyForCore(segDirectoryDetail);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
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

}
