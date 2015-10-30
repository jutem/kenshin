package com.kenshin.search.core.core;

import com.kenshin.search.core.index.manager.IndexerManager;
import com.kenshin.search.core.model.Model;
import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.ResourcePool;
import com.kenshin.search.core.util.CommonUtil;


/**
 * 负责启动
 */
public class KenshiCore {
	
	public static void main(String[] args) {
		//设置一个资源池
		ResourcePool resourcePool = new ResourcePool();
		
		//启动indexerManager
		IndexerManager indexManager = new IndexerManager(resourcePool);
		indexManager.start();
		
		//启动indexerReader
		ReaderManager readerManager = new ReaderManager(resourcePool);
		readerManager.start();
		
		Model model = new Model();
		model.setId(String.valueOf(CommonUtil.getUniqueId()));
		model.setFile1("test1");
		
		resourcePool.pushOriginData(model);
	}
}
