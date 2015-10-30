package com.kenshin.search.core.bak;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.kenshin.search.core.index.RamIndexer;
import com.kenshin.search.core.index.manager.IndexerManager;
import com.kenshin.search.core.model.Model;
import com.kenshin.search.core.reader.manager.ReaderManager;

public class KenshiCore {
	
	//内存分片ram
//	private static final List<Directory> bufferA = new ArrayList<Directory>();
//	private static final List<Directory> bufferB = new ArrayList<Directory>();
	
	//分词器
	private static final Analyzer analyzer = new StandardAnalyzer(); 
	
	private static final int MAX_INDEXER = 10; //启动索引者数量
	private static final ExecutorService indexerPool = Executors.newFixedThreadPool(MAX_INDEXER);
	
	private static final int DATAPOOLNUM = 10000; //每个生产者持有的数据
	private static final int MAX_PRODUCER = 10; //启动生产者数量
	private static final ExecutorService producerPool = Executors.newFixedThreadPool(MAX_PRODUCER);
	
	private static final int PER_MAX_SEARCHER = 0; //每秒搜索者数量
	
	//工作队列
	private static final LinkedBlockingQueue<Model> data = new LinkedBlockingQueue<Model>();
	
	private static final ExecutorService watcherPool = Executors.newSingleThreadExecutor(); //观察者
	
	private static final ScheduledExecutorService searcherPool = Executors.newSingleThreadScheduledExecutor(); //搜索者
	
	
	private static final String SEG_PATH = "D:/indexs/seg/";
	private static final String INDEX_PATH = "D:/indexs/index/"; 
	
	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		
		//启动watcher
		watcherPool.submit(new Runnable() {
			@Override
			public void run() {
				while(true) {
					try {
						System.out.println("<<<<<<<<<<<<<<<<<<< data size : " + data.size());
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
		
		//启动Manager
		final IndexerManager indexerManager = new IndexerManager(analyzer, INDEX_PATH);
		final ReaderManager searcherManager = new ReaderManager();
		
		//启动100个indexer
		for(int i = 0; i < MAX_INDEXER; i++) {
			RamIndexer indexer;
			try {
				indexer = new RamIndexer("IndexerNo_" + i, analyzer, data, SEG_PATH);
				indexerManager.registerIndexer(indexer);
				indexerPool.submit(indexer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("<<<<<<<<<<<<<<<<<<< all indexer have started");
		
		//初始化数据
		List<Producer> producers = new LinkedList<KenshiCore.Producer>();
		for(int i = 0; i < MAX_PRODUCER ; i++) {
			Queue<Model> littleData = new LinkedList<Model>();
			for(int j = 0; j < DATAPOOLNUM; j++) {
				Model model = new Model();
				model.setFile1("data test " + (System.currentTimeMillis() + j));
//				model.setFile1("datatest1");
				littleData.add(model);
			}
			Producer producer = new Producer("ProducerNo_" + i);
			producer.setLittleData(littleData);
			producers.add(producer);
		}
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<<< data all ready");
		
		//启动生产者
		for(Producer producer : producers) {
			producerPool.submit(producer);
		}
		
//		Thread.sleep(1000);
		
		//启动searcher
		searcherPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Map<String, String> searchMap = new LinkedHashMap<String, String>();
				searchMap.put("file1", "data");
				for (int i = 0; i < PER_MAX_SEARCHER; i++) {
					DirectoryPack pack = indexerManager.getAllDirectory();
//					List<Directory> allDirectory = pack.getAllDirectory();
					AbstractSearcher searcher1 = new RamSearcher("SearcherNo_" + i, searchMap, pack.getRamDirectories());
					AbstractSearcher searcher2 = new SegSearcher("SearcherNo_" + i, searchMap, pack.getSegDirectories());
					AbstractSearcher searcher3 = new CoreSearcher("SearcherNo_" + i, searchMap, pack.getCoreDirectory());
					searcherManager.submit(searcher1);
					searcherManager.submit(searcher2);
					searcherManager.submit(searcher3);
				}
			}
		}, 1, 1, TimeUnit.SECONDS);
		
		
		
		//启动search
//		final Searcher searcher = new Searcher("testSeacher", analyzer, null);
//		final Indexer testIndexer = indexerManager.getIndexers().get(0);
//		System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<< testIndexer : " + testIndexer.getIndexName());
//		seacherPool.scheduleWithFixedDelay(new Runnable() {
//			@Override
//			public void run() {
//				try {
//					searcher.ramQuery("test", testIndexer.getDirectory());
//				} catch (ParseException | IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}, 1, 2, TimeUnit.SECONDS);
	}
	
	//生产者
	private static class Producer implements Runnable {
		
		//生产者的部分数据
		private Queue<Model> littleData;
		private String name;
		private int count = 0;
		
		public Producer(String name) {
			super();
			this.name = name;
		}

		@Override
		public void run() {
			while(true) {
				Model model = littleData.poll();
				if(model == null)
					break;
				data.add(model);
				count ++;
				if(count > 5000) {
					try {
						count = 0;
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
//			System.out.println("<<<<<<<<<<<<<<<<<<< producer work over " + name);
		}
		
		public void setLittleData(Queue<Model> littleData) {
			this.littleData = littleData;
		}
	}
	
}
