package com.kenshin.search.core.core;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.kenshin.search.core.index.manager.IndexerManager;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.ResourcePool;


/**
 * 负责启动
 */
@Component
public class KenshiCore {
	
	private static final Logger logger = Logger.getLogger(KenshiCore.class);
	
	@Resource
	private ResourcePool resourcePool;
	
	@PostConstruct
	public void init() {
		//设置一个资源池
//		ResourcePool resourcePool = new ResourcePool();
		
		//启动indexerManager
		IndexerManager indexManager = new IndexerManager(resourcePool);
		indexManager.start();
		
		//启动indexerReader
		ReaderManager readerManager = new ReaderManager(resourcePool);
		readerManager.start();
		
//		List<Model> models = new LinkedList<Model>();
//		for(int i = 0; i < 1; i++) {
//			Model model = new Model();
////					model.setId(String.valueOf(CommonUtil.getUniqueId()));
//			model.setId("1");
//			model.setFile1("test1");
//			models.add(model);
//		}
//		for(Model model : models) {
//			resourcePool.pushOriginData(model);
//		}
		
//		Thread.sleep(1000);
//		for(Model model : models) {
//			Map<String, String> map = new LinkedHashMap<String, String>();
//			map.put(model.getId(), "id");
//			CommonQuery commonQuery = new CommonQuery(map);
//			List<Document> docs = readerManager.CommonQuery(commonQuery, 10);
//			Document doc = docs.get(0);
//			logger.debug("<<<<<<<<<<<<<<<<<<<<< docs 1 : " + Arrays.toString(doc.getValues("file1")));
//		}
	}
	
//	public static void main(String[] args) throws ParseException, IOException, InterruptedException {
//		//设置一个资源池
//		ResourcePool resourcePool = new ResourcePool();
//		
//		//启动indexerManager
//		IndexerManager indexManager = new IndexerManager(resourcePool);
//		indexManager.start();
//		
//		//启动indexerReader
//		ReaderManager readerManager = new ReaderManager(resourcePool);
//		readerManager.start();
//		
//		List<Model> models = new LinkedList<Model>();
//		for(int i = 0; i < 1; i++) {
//			Model model = new Model();
////			model.setId(String.valueOf(CommonUtil.getUniqueId()));
//			model.setId("1");
//			model.setFile1("test1");
//			models.add(model);
//		}
//		for(Model model : models) {
//			resourcePool.pushOriginData(model);
//		}
//		
//		Thread.sleep(1000);
//		for(Model model : models) {
//			Map<String, String> map = new LinkedHashMap<String, String>();
//			map.put(model.getId(), "id");
//			CommonQuery commonQuery = new CommonQuery(map);
//			List<Document> docs = readerManager.CommonQuery(commonQuery, 10);
//			Document doc = docs.get(0);
//			logger.debug("<<<<<<<<<<<<<<<<<<<<< docs 1 : " + Arrays.toString(doc.getValues("file1")));
//		}		
//	}
}
