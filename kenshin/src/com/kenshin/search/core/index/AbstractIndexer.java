package com.kenshin.search.core.index;

import com.kenshin.search.core.index.manager.IndexerManager;

public class AbstractIndexer{
	
	protected final String indexName; //索引唯一标识
	protected final IndexerManager indexerManager; //回调用的manager
	
	public AbstractIndexer(String indexName, IndexerManager indexerManager) {
		this.indexName = indexName;
		this.indexerManager = indexerManager;
	}
}
