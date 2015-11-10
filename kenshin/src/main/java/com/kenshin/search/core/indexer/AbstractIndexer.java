package com.kenshin.search.core.indexer;

import org.apache.lucene.analysis.Analyzer;

import com.kenshin.search.core.resource.ResourcePool;

public class AbstractIndexer{
	
	protected final String indexName; //标识名
	protected final ResourcePool resourcePool; //资源池
	protected final long ordinal; //标识号
	protected final long numberOfConsumers; //此类indexer的数量
	protected final Analyzer analyzer;
	
	public AbstractIndexer(String indexName, Analyzer analyzer, ResourcePool resourcePool, long ordinal, long numberOfConsumers) {
		this.indexName = indexName + "_" + ordinal;
		this.ordinal = ordinal;
		this.numberOfConsumers = numberOfConsumers;
		this.resourcePool = resourcePool;
		this.analyzer = analyzer;
	}
}
