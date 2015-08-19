package com.kenshin.search.core.search.manager;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.kenshin.search.core.search.AbstractSearcher;
import com.kenshin.search.core.search.CoreSearcher;

public class SearcherManager {
	
	private static final int MAX_SEACHER = 300;
	private ExecutorService pool = Executors.newFixedThreadPool(MAX_SEACHER);
	
	//提交一个searcher
	public void submit(AbstractSearcher abstractSearcher) {
		pool.submit(abstractSearcher);
	}
	
	public static void main(String[] args) throws IOException {
		
		Map<String, String> searchMap = new LinkedHashMap<String, String>();
		searchMap.put("file1", "data");
		
		Directory indexDirectory = FSDirectory.open(Paths.get("D:/indexs/index/"));
		AbstractSearcher searcher = new CoreSearcher("test", searchMap, indexDirectory);
		
		SearcherManager searcherManager = new SearcherManager();
		searcherManager.submit(searcher);
	}
}
