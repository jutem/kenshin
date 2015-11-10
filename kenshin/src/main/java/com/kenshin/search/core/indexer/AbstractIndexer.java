package com.kenshin.search.core.indexer;

import com.kenshin.search.core.resource.DisruptorResourcePool;

public class AbstractIndexer{
	
	protected final String indexName; //索引唯一标识
	protected final DisruptorResourcePool resourcePool; //资源池
	
	public AbstractIndexer(String indexName, DisruptorResourcePool resourcePool) {
		this.indexName = indexName;
		this.resourcePool = resourcePool;
	}
}
