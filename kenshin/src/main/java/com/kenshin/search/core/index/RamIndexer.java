package com.kenshin.search.core.index;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;

import com.kenshin.search.core.index.manager.IndexerManager;
import com.kenshin.search.core.model.Model;
import com.kenshin.search.core.model.directory.RAMDirectoryDetail;

public class RamIndexer extends AbstractIndexer implements Runnable{
	
	private static final Logger logger = Logger.getLogger(RamIndexer.class);
	
	private static final int threshold = 256 * 1024; // 最大内存大小 256k
	private static final double MAXRAMBUFFER = 256.0; //最大缓冲区256MB
	
	private RAMDirectoryDetail directoryDetail = new RAMDirectoryDetail(indexName);
	
	private Analyzer analyzer;
	private IndexWriter w;
	private IndexWriterConfig config;
	
	private final AtomicInteger count; //统计完成的索引数量

	public RamIndexer(String indexName, Analyzer analyzer, IndexerManager indexerManager, AtomicInteger count) throws IOException {
		super(indexName, indexerManager);
		this.analyzer = analyzer;
		
		this.config = new IndexWriterConfig(analyzer);
		this.config.setRAMBufferSizeMB(MAXRAMBUFFER); //256MB缓冲区
		this.w = new IndexWriter(directoryDetail.getDirectory(), config);
		
		this.count = count;
//		logger.debug("<<<<<<< config : bufferSize = " + config.getRAMBufferSizeMB() + " | openModel " + config.getOpenMode());
	}
	
	public void run() {
		while (true) {
			// 只读状态，说明在将索引写入硬盘
			try {
				RAMDirectory directory = directoryDetail.getDirectory();
				Model model = indexerManager.takeOriginData();;
				indexRAM(model);
				count.incrementAndGet();
				directoryDetail.addModelIds(model.getId());
				indexerManager.pushUpdateRam(directoryDetail);
				if (directory.ramBytesUsed() > threshold) { //变为只读模式的时候释放writer
					logger.debug("<<<<<<< ram byte used : " + directory.ramBytesUsed());
					w.close();
					indexerManager.pushReadyForSeg(directoryDetail);
					directoryDetail = new RAMDirectoryDetail(indexName);
					
					config = new IndexWriterConfig(analyzer); //重新获取writer
					config.setRAMBufferSizeMB(MAXRAMBUFFER);
					w = new IndexWriter(directoryDetail.getDirectory(), config);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// 写入RAM
	private void indexRAM(Model model) throws IOException {
		addDoc(w, model);
		w.commit();
//		w.close();
	}

	private void addDoc(IndexWriter w, Model model) throws IOException {
		Document doc = new Document();
		doc.add(new TextField("id", model.getId(), Field.Store.YES));
		doc.add(new TextField("file1", model.getFile1(), Field.Store.YES));
		w.addDocument(doc);
	}
	
	
}
