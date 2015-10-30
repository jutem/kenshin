package com.kenshin.search.core.bak;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;

import com.kenshin.search.core.model.Model;

public class RamIndexerBak implements Runnable{
	
	//分词器
	private final Analyzer analyzer;
	private final int threshold = 256 * 1024; // 最大内存大小 256k
	private final double MAXRAMBUFFER = 256.0; //最大缓冲区256MB
	private final LinkedBlockingQueue<Model> data;
	private final String segPath;
	private String indexName;
	private boolean readOnly = false;
	private RAMDirectory directory = new RAMDirectory();
	private RAMDirectory readDirectory = new RAMDirectory(); //当ram写入seg的时候，临时cp给
	private IndexWriter w;
	private IndexWriterConfig config;
	private Queue<Directory> segDirectories = new ConcurrentLinkedQueue<Directory>();
	
	private ExecutorService clearTask = Executors.newSingleThreadExecutor();

	public RamIndexerBak(String indexName, Analyzer analyzer, LinkedBlockingQueue<Model> data, String segPath) throws IOException {
		super();
		this.indexName = indexName;
		this.analyzer = analyzer;
		this.data = data;
		this.segPath = segPath;
		
		this.config = new IndexWriterConfig(analyzer);
		this.config.setRAMBufferSizeMB(MAXRAMBUFFER); //256MB缓冲区
		this.w = new IndexWriter(directory, config);
//		System.out.println("<<<<<<< config : bufferSize = " + config.getRAMBufferSizeMB() + " | openModel " + config.getOpenMode());
	}
	
	public void run() {
		while (true) {
			// 只读状态，说明在将索引写入硬盘
			try {
				if (readOnly) {
					System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<< " + indexName + " is readOnly ");
					Directory fsdDirectory = indexFile(directory, indexName);
					segDirectories.add(fsdDirectory);
					directory.close(); // 释放内存
					directory = new RAMDirectory(); //TODO重新申请内存块，看是否还有方法不需要重新初始化的
					config = new IndexWriterConfig(analyzer); //重新获取writer
					config.setRAMBufferSizeMB(MAXRAMBUFFER);
					w = new IndexWriter(directory, config);
					readOnly = false;
					readDirectory = new RAMDirectory();
				} else {
//					Date start = new Date();
//					System.out.println(indexName + " take and data size is : " + data.size());
					Model model = data.take();
					indexRAM(model);
//					System.out.println(indexName + " : ram used " + directory.ramBytesUsed()/1024);
					if (directory.ramBytesUsed() > threshold) { //变为只读模式的时候释放writer
						w.close();
						cpDirectory();
						readOnly = true;
					}
//					Date end = new Date();
//					System.out.println(indexName + " : " + (end.getTime() - start.getTime()) + " total milliseconds");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// 将RAM写入文件
	private Directory indexFile(Directory directory, String indexName)
			throws IOException {
		String fileName = segPath + indexName + "/" + indexName + "_" + System.currentTimeMillis();
		Directory fsdDirectory = FSDirectory.open(Paths.get(fileName));
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter wf = new IndexWriter(fsdDirectory, config);
		wf.addIndexes(directory);
		wf.close();
		
		return fsdDirectory;
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
	
	//当ram写入fsd时需要cp一份ram供读取
	private void cpDirectory() {
		for (String file : directory.listAll()) {
			try {
//				System.out.println("<<<<<<<<<<<<<<<<<<< directory list all : " + Arrays.toString(directory.listAll()));
				readDirectory.copyFrom(directory, file, file, IOContext.DEFAULT);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	//这里只清理seg,并没有清理实际的索引文件
	public void clearSegs(final Collection<Directory> directories) {
		clearTask.submit(new Runnable() {
			@Override
			public void run() {
				segDirectories.removeAll(directories);
			}
		});
	}

	/**获取当前可供读取的directory */
	public RAMDirectory getReadDirectory() {
		if(readOnly) {
			return readDirectory;
		}
		return directory;
	}

	public Queue<Directory> getSegDirectories() {
		return segDirectories;
	}
	
}
