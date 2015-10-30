package com.kenshin.search.core.index;

import java.io.IOException;

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
	
//	private static final int threshold = 256 * 1024; // 最大内存大小 256k
	private static final int threshold = 0;
	private static final double MAXRAMBUFFER = 256.0; //最大缓冲区256MB
	
	private RAMDirectoryDetail directoryDetail = new RAMDirectoryDetail(indexName);
	
	private Analyzer analyzer;
	private IndexWriter w;
	private IndexWriterConfig config;

	public RamIndexer(String indexName, Analyzer analyzer, IndexerManager indexerManager) throws IOException {
		super(indexName, indexerManager);
		this.analyzer = analyzer;
		
		this.config = new IndexWriterConfig(analyzer);
		this.config.setRAMBufferSizeMB(MAXRAMBUFFER); //256MB缓冲区
		this.w = new IndexWriter(directoryDetail.getDirectory(), config);
//		System.out.println("<<<<<<< config : bufferSize = " + config.getRAMBufferSizeMB() + " | openModel " + config.getOpenMode());
	}
	
	public void run() {
		while (true) {
			// 只读状态，说明在将索引写入硬盘
			try {
				RAMDirectory directory = directoryDetail.getDirectory();
				Model model = indexerManager.takeOriginData();;
				indexRAM(model);
				directoryDetail.addModelIds(model.getId());
				indexerManager.pushUpdateRam(directoryDetail);
				if (directory.ramBytesUsed() > threshold) { //变为只读模式的时候释放writer
					System.out.println("<<<<<<< ram byte used : " + directory.ramBytesUsed());
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
//		System.out.println("<<<<<< now the field : " + model.getFile1());
		doc.add(new TextField("file1", model.getFile1(), Field.Store.YES));
		w.addDocument(doc);
	}
	
	
}
