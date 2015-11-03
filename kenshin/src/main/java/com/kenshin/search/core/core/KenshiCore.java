package com.kenshin.search.core.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;

import com.kenshin.search.core.index.manager.IndexerManager;
import com.kenshin.search.core.model.Model;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.reader.query.CommonQuery;
import com.kenshin.search.core.resource.ResourcePool;
import com.kenshin.search.core.util.CommonUtil;


/**
 * 负责启动
 */
public class KenshiCore {
	
	public static void main(String[] args) throws ParseException, IOException, InterruptedException {
		//设置一个资源池
		ResourcePool resourcePool = new ResourcePool();
		
		//启动indexerManager
		IndexerManager indexManager = new IndexerManager(resourcePool);
		indexManager.start();
		
		//启动indexerReader
		ReaderManager readerManager = new ReaderManager(resourcePool);
		readerManager.start();
		
		List<Model> models = new LinkedList<Model>();
		for(int i = 0; i < 1; i++) {
			Model model = new Model();
//			model.setId(String.valueOf(CommonUtil.getUniqueId()));
			model.setId("1");
			model.setFile1("test1");
			models.add(model);
		}
		for(Model model : models) {
			resourcePool.pushOriginData(model);
		}
		
		Thread.sleep(1000);
		for(Model model : models) {
			Map<String, String> map = new LinkedHashMap<String, String>();
			map.put(model.getId(), "id");
			CommonQuery commonQuery = new CommonQuery(map);
			List<Document> docs = readerManager.CommonQuery(commonQuery, 10);
			Document doc = docs.get(0);
			System.out.println("<<<<<<<<<<<<<<<<<<<<< docs 1 : " + Arrays.toString(doc.getValues("file1")));
		}		
		
		
	}
}
