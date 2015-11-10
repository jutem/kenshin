package com.kenshin.search.core.reader;

import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.ResourcePool;

public abstract class AbstractReader {
	
	protected final ResourcePool resourcePool;
	protected final ReaderManager readerManager;
	protected final long ordinal; //标识号
	protected final long numberOfConsumers; //此类indexer的数量
	
	public AbstractReader(ResourcePool resourcePool, ReaderManager readerManager, long ordinal, long numberOfConsumers) {
		super();
		this.resourcePool = resourcePool;
		this.readerManager = readerManager;
		this.ordinal = ordinal;
		this.numberOfConsumers = numberOfConsumers;
	}
	
}
